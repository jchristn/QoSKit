namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Metrics;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Telemetry suite: verifies the QoSKit meter emits and matches the synchronous statistics.
    /// </summary>
    public static class TelemetrySuites
    {
        /// <summary>The telemetry suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Telemetry", "CountersMatchStats", "Meter counters match the statistics snapshot", _ =>
            {
                string queueName = "meter-" + Guid.NewGuid().ToString("N");
                long enqueued = 0;
                long dequeued = 0;

                using MeterListener listener = new MeterListener();
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == QoSMetrics.MeterName)
                        l.EnableMeasurementEvents(instrument);
                };
                listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
                {
                    if (!TagMatches(tags, queueName))
                        return;
                    if (instrument.Name == "qoskit.queue.enqueued")
                        Interlocked.Add(ref enqueued, measurement);
                    else if (instrument.Name == "qoskit.queue.dequeued")
                        Interlocked.Add(ref dequeued, measurement);
                });
                listener.Start();

                QoSQueueOptions options = new QoSQueueOptions { Name = queueName };
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(options);
                for (int i = 0; i < 100; i++)
                    queue.Enqueue(i);
                for (int i = 0; i < 60; i++)
                    queue.Dequeue();

                listener.Dispose();

                QoSQueueStatistics stats = queue.Statistics;
                Check.Equal(stats.Enqueued, Interlocked.Read(ref enqueued), "enqueued counter matches stats");
                Check.Equal(stats.Dequeued, Interlocked.Read(ref dequeued), "dequeued counter matches stats");
                Check.Equal(100L, Interlocked.Read(ref enqueued), "100 enqueued observed");
                Check.Equal(60L, Interlocked.Read(ref dequeued), "60 dequeued observed");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "DisabledEmitsNothing", "Metrics disabled emits no measurements", _ =>
            {
                string queueName = "nometer-" + Guid.NewGuid().ToString("N");
                long observed = 0;

                using MeterListener listener = new MeterListener();
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == QoSMetrics.MeterName)
                        l.EnableMeasurementEvents(instrument);
                };
                listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
                {
                    if (TagMatches(tags, queueName))
                        Interlocked.Add(ref observed, measurement);
                });
                listener.Start();

                QoSQueueOptions options = new QoSQueueOptions { Name = queueName, EnableMetrics = false };
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(options);
                for (int i = 0; i < 50; i++)
                    queue.Enqueue(i);
                queue.Dequeue();

                listener.Dispose();
                Check.Equal(0L, Interlocked.Read(ref observed), "no measurements when metrics disabled");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Telemetry", "Telemetry", cases);
        }

        private static bool TagMatches(ReadOnlySpan<KeyValuePair<string, object?>> tags, string queueName)
        {
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "queue.name" && tag.Value is string name && name == queueName)
                    return true;
            }

            return false;
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
