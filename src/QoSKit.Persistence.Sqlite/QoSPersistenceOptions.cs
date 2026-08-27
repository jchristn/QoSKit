namespace QoSKit.Persistence.Sqlite
{
    using System;
    using QoSKit;

    /// <summary>
    /// Configuration for a <see cref="SqliteQoSStore{T}"/>. Use the <see cref="GuaranteedDelivery"/>
    /// and <see cref="Fast"/> presets for the two sensible ends of the durability spectrum.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class QoSPersistenceOptions<T>
    {
        private int _BatchSize = 256;
        private string _TableName = "qos_items";

        /// <summary>The database file path. Never null or empty.</summary>
        public string FilePath { get; }

        /// <summary>The serializer for payloads. Defaults to a JSON serializer.</summary>
        public IQoSSerializer<T> Serializer { get; set; }

        /// <summary>The durability mode. Default is <see cref="PersistenceMode.WriteBehind"/>.</summary>
        public PersistenceMode Mode { get; set; } = PersistenceMode.WriteBehind;

        /// <summary>The synchronous pragma used in <see cref="PersistenceMode.Durable"/>. Default <see cref="SqliteSynchronous.Full"/>.</summary>
        public SqliteSynchronous Synchronous { get; set; } = SqliteSynchronous.Full;

        /// <summary>The number of write-behind operations to batch per commit. Default 256, minimum 1.</summary>
        /// <exception cref="ArgumentOutOfRangeException">The value is less than 1.</exception>
        public int BatchSize
        {
            get { return _BatchSize; }
            set
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), "BatchSize must be at least 1.");
                _BatchSize = value;
            }
        }

        /// <summary>The table name used for this queue's items. Default "qos_items".</summary>
        /// <exception cref="ArgumentException">The value is null, empty, or whitespace.</exception>
        public string TableName
        {
            get { return _TableName; }
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                    throw new ArgumentException("TableName must not be null, empty, or whitespace.", nameof(value));
                _TableName = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSPersistenceOptions{T}"/> class.
        /// </summary>
        /// <param name="filePath">The database file path.</param>
        /// <param name="serializer">The payload serializer; when null, a JSON serializer is used.</param>
        /// <exception cref="ArgumentException"><paramref name="filePath"/> is null, empty, or whitespace.</exception>
        public QoSPersistenceOptions(string filePath, IQoSSerializer<T>? serializer = null)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("FilePath must not be null, empty, or whitespace.", nameof(filePath));
            FilePath = filePath;
            Serializer = serializer ?? new JsonQoSSerializer<T>();
        }

        /// <summary>
        /// Returns options for guaranteed delivery: durable commits with a full synchronous pragma.
        /// No acknowledged enqueue is lost across a restart.
        /// </summary>
        /// <param name="filePath">The database file path.</param>
        /// <param name="serializer">The payload serializer; when null, a JSON serializer is used.</param>
        /// <returns>The configured options.</returns>
        public static QoSPersistenceOptions<T> GuaranteedDelivery(string filePath, IQoSSerializer<T>? serializer = null)
        {
            return new QoSPersistenceOptions<T>(filePath, serializer)
            {
                Mode = PersistenceMode.Durable,
                Synchronous = SqliteSynchronous.Full
            };
        }

        /// <summary>
        /// Returns options for fast persistence: write-behind batching with a small loss window on a
        /// hard crash.
        /// </summary>
        /// <param name="filePath">The database file path.</param>
        /// <param name="serializer">The payload serializer; when null, a JSON serializer is used.</param>
        /// <returns>The configured options.</returns>
        public static QoSPersistenceOptions<T> Fast(string filePath, IQoSSerializer<T>? serializer = null)
        {
            return new QoSPersistenceOptions<T>(filePath, serializer)
            {
                Mode = PersistenceMode.WriteBehind
            };
        }
    }
}
