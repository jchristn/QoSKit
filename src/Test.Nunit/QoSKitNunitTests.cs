namespace Test.Nunit
{
    using System.Collections;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Test.Shared;
    using Touchstone.Core;
    using Touchstone.NunitAdapter;

    /// <summary>
    /// Runs each Touchstone descriptor as a separate NUnit test case for per-test visibility.
    /// </summary>
    [TestFixture]
    public sealed class QoSKitNunitTests
    {
        private static IEnumerable TestCases()
        {
            return new TouchstoneTestCaseSource(QoSKitSuites.All);
        }

        /// <summary>Runs a single descriptor.</summary>
        /// <param name="testCase">The descriptor.</param>
        /// <returns>A task that completes when the case has run.</returns>
        [Test]
        [TestCaseSource(nameof(TestCases))]
        public async Task RunTest(TestCaseDescriptor testCase)
        {
            await testCase.ExecuteAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
