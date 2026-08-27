namespace Test.Shared
{
    using System.Collections.Generic;
    using Touchstone.Core;

    /// <summary>
    /// Aggregates every QoSKit test suite so runners consume them with a single call.
    /// </summary>
    public static class QoSKitSuites
    {
        /// <summary>Gets all suites.</summary>
        public static IReadOnlyList<TestSuiteDescriptor> All
        {
            get
            {
                return new List<TestSuiteDescriptor>
                {
                    CoreSuites.ContractSuite(),
                    SharedContractSuite.Suite(),
                    CoreSuites.FifoSuite(),
                    CoreSuites.LifoSuite(),
                    DisciplineSuites.PrioritySuite(),
                    DisciplineSuites.WeightedFairSuite(),
                    DisciplineSuites.ClassBasedSuite(),
                    DisciplineSuites.LowLatencySuite(),
                    DisciplineSuites.WeightedRoundRobinSuite(),
                    ClassificationSuites.Suite(),
                    ChainingSuites.Suite(),
                    TelemetrySuites.Suite(),
                    LivenessSuites.Suite(),
                    PersistenceSuites.Suite(),
                    CrashSuites.Suite(),
                    SoakSuites.Suite()
                };
            }
        }
    }
}
