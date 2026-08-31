namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using QoSKit;

    /// <summary>
    /// Test-only helpers that capture QoSKit metric measurements and trace spans while a body runs, so
    /// suites can assert on the emitted telemetry. Not part of the shipping library.
    /// </summary>
    public static class TelemetryCapture
    {
        /// <summary>
        /// Runs <paramref name="body"/> while subscribed to the QoSKit meter, returning every push
        /// measurement (counters, up-down counters, histograms) whose <c>queue.name</c> tag matches
        /// <paramref name="queueName"/>. When <paramref name="recordObservable"/> is true, observable
        /// gauges are collected once after the body runs; the caller must keep the queue alive across
        /// the call because the meter holds only a weak reference to it.
        /// </summary>
        /// <param name="queueName">The queue name to filter measurements by.</param>
        /// <param name="body">The action that exercises the queue.</param>
        /// <param name="recordObservable">Whether to collect observable gauges after the body runs.</param>
        /// <returns>The captured measurements in emission order.</returns>
        public static List<TelemetryMeasurement> CaptureMetrics(string queueName, Action body, bool recordObservable = false)
        {
            if (queueName == null)
                throw new ArgumentNullException(nameof(queueName));
            if (body == null)
                throw new ArgumentNullException(nameof(body));

            List<TelemetryMeasurement> captured = new List<TelemetryMeasurement>();
            object gate = new object();

            using (MeterListener listener = new MeterListener())
            {
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == QoSMetrics.MeterName)
                        l.EnableMeasurementEvents(instrument);
                };
                listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
                    Record(captured, gate, queueName, instrument.Name, measurement, tags));
                listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
                    Record(captured, gate, queueName, instrument.Name, measurement, tags));
                listener.Start();

                body();

                if (recordObservable)
                    listener.RecordObservableInstruments();
            }

            return captured;
        }

        /// <summary>
        /// Runs <paramref name="body"/> while subscribed to the QoSKit activity source, returning every
        /// span that stopped during the body, with its operation name and tags.
        /// </summary>
        /// <param name="body">The action that exercises the queue or pipeline.</param>
        /// <returns>The captured spans in completion order.</returns>
        public static List<TelemetryMeasurement> CaptureSpans(Action body)
        {
            if (body == null)
                throw new ArgumentNullException(nameof(body));

            List<TelemetryMeasurement> spans = new List<TelemetryMeasurement>();
            object gate = new object();

            ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == QoSTracing.ActivitySourceName,
                Sample = SampleAllData,
                SampleUsingParentId = SampleAllDataByParentId,
                ActivityStopped = activity =>
                {
                    Dictionary<string, string?> tags = new Dictionary<string, string?>();
                    foreach (KeyValuePair<string, object?> tag in activity.TagObjects)
                        tags[tag.Key] = tag.Value?.ToString();
                    lock (gate)
                        spans.Add(new TelemetryMeasurement(activity.OperationName, 0, tags));
                }
            };

            ActivitySource.AddActivityListener(listener);
            try
            {
                body();
            }
            finally
            {
                listener.Dispose();
            }

            return spans;
        }

        private static ActivitySamplingResult SampleAllData(ref ActivityCreationOptions<ActivityContext> options)
        {
            return ActivitySamplingResult.AllData;
        }

        private static ActivitySamplingResult SampleAllDataByParentId(ref ActivityCreationOptions<string> options)
        {
            return ActivitySamplingResult.AllData;
        }

        private static void Record(
            List<TelemetryMeasurement> captured,
            object gate,
            string queueName,
            string instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            Dictionary<string, string?> map = new Dictionary<string, string?>();
            bool matches = false;
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                string? rendered = tag.Value?.ToString();
                map[tag.Key] = rendered;
                if (tag.Key == QoSMetrics.TagQueueName && rendered == queueName)
                    matches = true;
            }

            if (!matches)
                return;

            lock (gate)
                captured.Add(new TelemetryMeasurement(instrument, value, map));
        }
    }
}
