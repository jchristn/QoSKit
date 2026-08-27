namespace Test.Nunit
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Test.Shared;
    using Touchstone.Core;
    using Touchstone.NunitAdapter;

    /// <summary>
    /// Runs every Touchstone descriptor sequentially through the executor in a single test.
    /// </summary>
    [TestFixture]
    public sealed class QoSKitNunitFactTests : TouchstoneNunitBase
    {
        /// <inheritdoc/>
        protected override IReadOnlyList<TestSuiteDescriptor> Suites
        {
            get { return QoSKitSuites.All; }
        }

        /// <summary>Runs all suites.</summary>
        /// <returns>A task that completes when all suites have run.</returns>
        [Test]
        public async Task RunAll()
        {
            await RunAllAsync().ConfigureAwait(false);
        }
    }
}
