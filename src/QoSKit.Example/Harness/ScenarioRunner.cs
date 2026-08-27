namespace QoSKit.Example.Harness
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit.Example.Scenarios;

    /// <summary>
    /// Drives a scenario for a fixed window: prints its banner, runs producer/consumer work through a
    /// reporter, prints a live counter, and renders the summary. Owns all timing and presentation.
    /// </summary>
    public static class ScenarioRunner
    {
        /// <summary>
        /// Runs a scenario for the given window and prints its banner, live totals, and summary.
        /// </summary>
        /// <param name="scenario">The scenario to run.</param>
        /// <param name="window">How long to run.</param>
        /// <returns>A task that completes when the scenario and its summary are done.</returns>
        public static async Task RunAsync(IDemoScenario scenario, TimeSpan window)
        {
            Console.WriteLine();
            Console.WriteLine(new string('=', 78));
            WriteColored($"  {scenario.Name}", ConsoleColor.Cyan);
            Console.WriteLine($"  {scenario.Description}");
            Console.WriteLine($"  Running for {window.TotalSeconds:N0} seconds...");
            Console.WriteLine(new string('=', 78));

            ConsoleReporter reporter = new ConsoleReporter();
            using CancellationTokenSource cts = new CancellationTokenSource(window);

            Task live = Task.Run(async () =>
            {
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        reporter.PrintLiveLine();
                        await Task.Delay(250, cts.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });

            try
            {
                await scenario.RunAsync(reporter, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            await live.ConfigureAwait(false);
            reporter.PrintSummary();
        }

        private static void WriteColored(string text, ConsoleColor color)
        {
            if (Console.IsOutputRedirected)
            {
                Console.WriteLine(text);
                return;
            }

            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = previous;
        }
    }
}
