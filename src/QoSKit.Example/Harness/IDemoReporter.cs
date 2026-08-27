namespace QoSKit.Example.Harness
{
    /// <summary>
    /// The seam between a scenario's queuing logic and the console. Scenarios call these methods to
    /// report what happened; the reporter decides how to render it. Scenarios never touch the console.
    /// </summary>
    public interface IDemoReporter
    {
        /// <summary>Reports that an item was enqueued.</summary>
        /// <param name="label">A short label grouping the item (e.g. its tenant or priority).</param>
        void Enqueued(string label);

        /// <summary>Reports that an item was dequeued and serviced.</summary>
        /// <param name="label">A short label grouping the item.</param>
        void Serviced(string label);

        /// <summary>Reports a free-form note about what the scenario is doing.</summary>
        /// <param name="message">The note.</param>
        void Note(string message);
    }
}
