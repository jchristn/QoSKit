namespace QoSKit
{
    using System.Collections.Generic;

    /// <summary>
    /// Thrown when a pipeline is started or validated and its links form a cycle, including a queue
    /// linked to itself.
    /// </summary>
    public sealed class PipelineCycleException : QoSException
    {
        /// <summary>
        /// The names of the queues on the detected cycle path, in order. Never null.
        /// </summary>
        public IReadOnlyList<string> CyclePath { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PipelineCycleException"/> class.
        /// </summary>
        /// <param name="cyclePath">The names of the queues on the cycle path.</param>
        public PipelineCycleException(IReadOnlyList<string> cyclePath)
            : base($"The pipeline link graph contains a cycle: {string.Join(" -> ", cyclePath ?? new List<string>())}.")
        {
            CyclePath = cyclePath ?? new List<string>();
        }
    }
}
