namespace Test.Xunit
{
    using System.Threading;
    using System.Threading.Tasks;
    using Test.Shared;
    using Touchstone.Core;

    /// <summary>
    /// Runs each Touchstone descriptor as a separate xUnit theory row for per-test visibility.
    /// </summary>
    public sealed class QoSKitTheoryTests
    {
        private readonly ITestOutputHelper _Output;

        /// <summary>Initializes a new instance of the <see cref="QoSKitTheoryTests"/> class.</summary>
        /// <param name="output">The xUnit output helper.</param>
        public QoSKitTheoryTests(ITestOutputHelper output)
        {
            _Output = output;
        }

        /// <summary>Provides one row per non-skipped descriptor.</summary>
        /// <returns>The theory data.</returns>
        public static TheoryData<TestCaseDescriptor> TestCases()
        {
            TheoryData<TestCaseDescriptor> data = new TheoryData<TestCaseDescriptor>();
            foreach (TestSuiteDescriptor suite in QoSKitSuites.All)
            {
                foreach (TestCaseDescriptor testCase in suite.Cases)
                {
                    if (!testCase.Skip)
                        data.Add(testCase);
                }
            }

            return data;
        }

        /// <summary>Runs a single descriptor.</summary>
        /// <param name="testCase">The descriptor.</param>
        /// <returns>A task that completes when the case has run.</returns>
        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task RunTest(TestCaseDescriptor testCase)
        {
            _Output.WriteLine("Running: " + testCase.DisplayName);
            await testCase.ExecuteAsync(CancellationToken.None);
        }
    }
}
