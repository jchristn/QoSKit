using Xunit;

// These are integration-style tests (real queues, timing, concurrency, and endurance soaks). Run
// them sequentially, like the console and NUnit runners, so parallel test collections do not
// contend for CPU and make the timing-sensitive cases flaky on constrained CI runners.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
