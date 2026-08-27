namespace Test.Shared
{
    using System;
    using QoSKit;

    /// <summary>
    /// Pairs a discipline name with a constructor delegate, so the shared contract can run against
    /// every discipline from one descriptor set.
    /// </summary>
    public sealed class QueueFactory
    {
        /// <summary>The discipline name.</summary>
        public string Name { get; }

        /// <summary>Creates a queue instance, optionally with the given options.</summary>
        public Func<QoSQueueOptions?, IQoSQueue<int>> Create { get; }

        /// <summary>Initializes a new instance of the <see cref="QueueFactory"/> class.</summary>
        /// <param name="name">The discipline name.</param>
        /// <param name="create">The constructor delegate.</param>
        public QueueFactory(string name, Func<QoSQueueOptions?, IQoSQueue<int>> create)
        {
            Name = name;
            Create = create;
        }
    }
}
