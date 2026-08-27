namespace QoSKit.Persistence.Sqlite
{
    /// <summary>
    /// The SQLite <c>synchronous</c> pragma level, controlling how aggressively writes are flushed.
    /// </summary>
    public enum SqliteSynchronous
    {
        /// <summary>Fastest; can lose the last transaction on OS crash or power loss.</summary>
        Normal = 0,

        /// <summary>Flushes at critical moments; safe against OS crash and power loss with WAL.</summary>
        Full = 1,

        /// <summary>Like <see cref="Full"/> with an extra durability sync on commit.</summary>
        Extra = 2
    }
}
