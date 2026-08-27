using System;
using System.Threading.Tasks;
using QoSKit.Example.Harness;
using QoSKit.Example.Scenarios;

Console.WriteLine();
Console.WriteLine("QoSKit demo - three scenarios, ten seconds each.");

IDemoScenario[] scenarios =
{
    new PriorityScenario(),
    new WeightedFairScenario(),
    new ChainedScenario()
};

foreach (IDemoScenario scenario in scenarios)
{
    await ScenarioRunner.RunAsync(scenario, TimeSpan.FromSeconds(10));
}

Console.WriteLine("Done.");
return 0;
