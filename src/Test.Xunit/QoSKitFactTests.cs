namespace Test.Xunit
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Test.Shared;
    using Touchstone.Core;
    using Touchstone.XunitAdapter;

    /// <summary>
    /// Runs every Touchstone descriptor sequentially through the executor in a single fact.
    /// </summary>
    public sealed class QoSKitFactTests : TouchstoneFactBase
    {
        /// <inheritdoc/>
        protected override IReadOnlyList<TestSuiteDescriptor> Suites
        {
            get { return QoSKitSuites.All; }
        }

        /// <summary>Runs all suites.</summary>
        /// <returns>A task that completes when all suites have run.</returns>
        [Fact]
        public async Task RunAll()
        {
            await RunAllAsync();
        }
    }
}
