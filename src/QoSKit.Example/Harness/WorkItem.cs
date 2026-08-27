namespace QoSKit.Example.Harness
{
    /// <summary>
    /// A unit of work flowing through the demo queues. Stands in for something like an inference
    /// request dispatched to a model server.
    /// </summary>
    public sealed class WorkItem
    {
        /// <summary>A unique identifier.</summary>
        public int Id { get; set; }

        /// <summary>The tenant or flow this work belongs to.</summary>
        public string Tenant { get; set; } = "default";

        /// <summary>A priority band (lower is more urgent).</summary>
        public int Priority { get; set; }

        /// <summary>Whether this is interactive (latency-sensitive) work.</summary>
        public bool Interactive { get; set; }

        /// <summary>Initializes a new instance of the <see cref="WorkItem"/> class.</summary>
        /// <param name="id">The identifier.</param>
        public WorkItem(int id)
        {
            Id = id;
        }
    }
}
