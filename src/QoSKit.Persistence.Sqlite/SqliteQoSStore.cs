namespace QoSKit.Persistence.Sqlite
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Data.Sqlite;
    using QoSKit;

    /// <summary>
    /// A SQLite-backed durability journal for a queue. Records item adds and removes to a table and
    /// replays them on recovery. In <see cref="PersistenceMode.Durable"/> mode each write is committed
    /// before returning; in <see cref="PersistenceMode.WriteBehind"/> mode writes are batched.
    /// Serializes its own database access; safe to share with one queue's enqueue and dequeue paths.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class SqliteQoSStore<T> : IQoSStore<T>
    {
        private readonly object _Lock = new object();
        private readonly QoSPersistenceOptions<T> _Options;
        private readonly SqliteConnection _Connection;
        private readonly string _Table;
        private readonly int _CommitThreshold;
        private SqliteTransaction? _Transaction;
        private int _OpsSinceCommit;
        private bool _Disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteQoSStore{T}"/> class, opening or creating
        /// the database and its table.
        /// </summary>
        /// <param name="options">The store configuration.</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
        public SqliteQoSStore(QoSPersistenceOptions<T> options)
        {
            _Options = options ?? throw new ArgumentNullException(nameof(options));
            _Table = options.TableName;
            _CommitThreshold = options.Mode == PersistenceMode.Durable ? 1 : options.BatchSize;

            SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder
            {
                DataSource = options.FilePath
            };
            _Connection = new SqliteConnection(builder.ToString());
            _Connection.Open();

            string synchronous = options.Mode == PersistenceMode.Durable ? MapSynchronous(options.Synchronous) : "NORMAL";
            Execute($"PRAGMA journal_mode=WAL; PRAGMA synchronous={synchronous};");
            Execute($"CREATE TABLE IF NOT EXISTS {_Table} (seq INTEGER PRIMARY KEY, payload BLOB NOT NULL);");
        }

        /// <inheritdoc/>
        public object? Prepare(T item)
        {
            return _Options.Serializer.Serialize(item);
        }

        /// <inheritdoc/>
        public void Commit(long sequence, object? prepared)
        {
            byte[] payload = (byte[])prepared!;
            lock (_Lock)
            {
                ThrowIfDisposed();
                EnsureTransaction();
                using SqliteCommand command = _Connection.CreateCommand();
                command.Transaction = _Transaction;
                command.CommandText = $"INSERT OR REPLACE INTO {_Table} (seq, payload) VALUES ($seq, $payload);";
                command.Parameters.AddWithValue("$seq", sequence);
                command.Parameters.AddWithValue("$payload", payload);
                command.ExecuteNonQuery();
                CommitIfThreshold();
            }
        }

        /// <inheritdoc/>
        public void Remove(long sequence)
        {
            lock (_Lock)
            {
                ThrowIfDisposed();
                EnsureTransaction();
                using SqliteCommand command = _Connection.CreateCommand();
                command.Transaction = _Transaction;
                command.CommandText = $"DELETE FROM {_Table} WHERE seq = $seq;";
                command.Parameters.AddWithValue("$seq", sequence);
                command.ExecuteNonQuery();
                CommitIfThreshold();
            }
        }

        /// <inheritdoc/>
        public void Clear()
        {
            lock (_Lock)
            {
                ThrowIfDisposed();
                CommitPending();
                Execute($"DELETE FROM {_Table};");
            }
        }

        /// <inheritdoc/>
        public void Flush()
        {
            lock (_Lock)
            {
                if (_Disposed)
                    return;
                CommitPending();
            }
        }

        /// <inheritdoc/>
        public IReadOnlyList<QoSPersistedItem<T>> Recover()
        {
            lock (_Lock)
            {
                ThrowIfDisposed();
                CommitPending();
                List<QoSPersistedItem<T>> items = new List<QoSPersistedItem<T>>();
                using SqliteCommand command = _Connection.CreateCommand();
                command.CommandText = $"SELECT seq, payload FROM {_Table} ORDER BY seq ASC;";
                using SqliteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    long sequence = reader.GetInt64(0);
                    byte[] payload = (byte[])reader[1];
                    T item = _Options.Serializer.Deserialize(payload);
                    items.Add(new QoSPersistedItem<T>(sequence, item));
                }

                return items;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_Lock)
            {
                if (_Disposed)
                    return;
                _Disposed = true;
                CommitPending();
                try
                {
                    Execute("PRAGMA wal_checkpoint(TRUNCATE);");
                }
                catch (SqliteException)
                {
                }

                _Connection.Dispose();
            }
        }

        private void EnsureTransaction()
        {
            _Transaction ??= _Connection.BeginTransaction();
        }

        private void CommitIfThreshold()
        {
            _OpsSinceCommit++;
            if (_OpsSinceCommit >= _CommitThreshold)
                CommitPending();
        }

        private void CommitPending()
        {
            if (_Transaction != null)
            {
                _Transaction.Commit();
                _Transaction.Dispose();
                _Transaction = null;
            }

            _OpsSinceCommit = 0;
        }

        private void Execute(string sql)
        {
            using SqliteCommand command = _Connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private void ThrowIfDisposed()
        {
            if (_Disposed)
                throw new ObjectDisposedException(nameof(SqliteQoSStore<T>));
        }

        private static string MapSynchronous(SqliteSynchronous synchronous)
        {
            switch (synchronous)
            {
                case SqliteSynchronous.Normal:
                    return "NORMAL";
                case SqliteSynchronous.Extra:
                    return "EXTRA";
                default:
                    return "FULL";
            }
        }
    }
}
