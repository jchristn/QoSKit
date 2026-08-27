namespace QoSKit
{
    /// <summary>
    /// An immutable point-in-time snapshot of a queue's counters. Obtain a fresh snapshot from a
    /// queue's <c>Statistics</c> property; the values do not update after capture.
    /// </summary>
    public sealed class QoSQueueStatistics
    {
        /// <summary>
        /// The total number of items successfully enqueued over the life of the queue.
        /// </summary>
        public long Enqueued { get; }

        /// <summary>
        /// The total number of items dequeued over the life of the queue.
        /// </summary>
        public long Dequeued { get; }

        /// <summary>
        /// The total number of items dropped (by an overflow, unknown-class, or unroutable policy).
        /// </summary>
        public long Dropped { get; }

        /// <summary>
        /// The total number of items rejected (a full queue under <see cref="OverflowPolicy.Reject"/>).
        /// </summary>
        public long Rejected { get; }

        /// <summary>
        /// The number of items currently resident in the queue.
        /// </summary>
        public int CurrentDepth { get; }

        /// <summary>
        /// The high-water mark of <see cref="CurrentDepth"/> observed over the life of the queue.
        /// </summary>
        public int PeakDepth { get; }

        /// <summary>
        /// The mean wait time, in milliseconds, between enqueue and dequeue across all dequeued items.
        /// Zero when nothing has been dequeued.
        /// </summary>
        public double AverageWaitMilliseconds { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSQueueStatistics"/> class.
        /// </summary>
        /// <param name="enqueued">Total enqueued.</param>
        /// <param name="dequeued">Total dequeued.</param>
        /// <param name="dropped">Total dropped.</param>
        /// <param name="rejected">Total rejected.</param>
        /// <param name="currentDepth">Current resident count.</param>
        /// <param name="peakDepth">Peak resident count.</param>
        /// <param name="averageWaitMilliseconds">Mean wait time in milliseconds.</param>
        public QoSQueueStatistics(
            long enqueued,
            long dequeued,
            long dropped,
            long rejected,
            int currentDepth,
            int peakDepth,
            double averageWaitMilliseconds)
        {
            Enqueued = enqueued;
            Dequeued = dequeued;
            Dropped = dropped;
            Rejected = rejected;
            CurrentDepth = currentDepth;
            PeakDepth = peakDepth;
            AverageWaitMilliseconds = averageWaitMilliseconds;
        }
    }
}
