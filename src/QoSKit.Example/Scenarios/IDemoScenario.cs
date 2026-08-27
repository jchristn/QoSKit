namespace QoSKit.Example.Scenarios
{
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit.Example.Harness;

    /// <summary>
    /// A self-contained demo scenario. Its <see cref="RunAsync"/> contains only queuing code and
    /// reports activity through an <see cref="IDemoReporter"/>; it never touches the console.
    /// </summary>
    public interface IDemoScenario
    {
        /// <summary>The scenario name shown in the banner.</summary>
        string Name { get; }

        /// <summary>A one-line description of what the scenario demonstrates.</summary>
        string Description { get; }

        /// <summary>
        /// Runs the scenario until the token is cancelled (the demo window closes).
        /// </summary>
        /// <param name="reporter">The reporter to report enqueue/service activity to.</param>
        /// <param name="cancellationToken">Cancelled when the demo window closes.</param>
        /// <returns>A task that completes when the scenario stops.</returns>
        Task RunAsync(IDemoReporter reporter, CancellationToken cancellationToken);
    }
}
