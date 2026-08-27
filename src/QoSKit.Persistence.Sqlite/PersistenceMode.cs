namespace QoSKit.Persistence.Sqlite
{
    /// <summary>
    /// The durability ordering for a SQLite-backed store.
    /// </summary>
    public enum PersistenceMode
    {
        /// <summary>
        /// Writes are batched and committed on a threshold or on flush. Near in-memory throughput,
        /// but items written after the last commit are lost on a hard crash.
        /// </summary>
        WriteBehind = 0,

        /// <summary>
        /// Every write is committed to stable storage before the call returns. No loss once a call
        /// returns, at the cost of a disk commit per operation (mitigated by grouping).
        /// </summary>
        Durable = 1
    }
}
