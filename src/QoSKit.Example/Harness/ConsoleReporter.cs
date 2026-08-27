namespace QoSKit.Example.Harness
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// Renders scenario activity to the console: running totals during the window and a per-label
    /// summary table at the end. All console formatting lives here, isolated from queuing code.
    /// </summary>
    public sealed class ConsoleReporter : IDemoReporter
    {
        private readonly object _Lock = new object();
        private readonly Dictionary<string, long> _Enqueued = new Dictionary<string, long>();
        private readonly Dictionary<string, long> _Serviced = new Dictionary<string, long>();
        private long _TotalEnqueued;
        private long _TotalServiced;

        /// <inheritdoc/>
        public void Enqueued(string label)
        {
            lock (_Lock)
            {
                _Enqueued.TryGetValue(label, out long c);
                _Enqueued[label] = c + 1;
                _TotalEnqueued++;
            }
        }

        /// <inheritdoc/>
        public void Serviced(string label)
        {
            lock (_Lock)
            {
                _Serviced.TryGetValue(label, out long c);
                _Serviced[label] = c + 1;
                _TotalServiced++;
            }
        }

        /// <inheritdoc/>
        public void Note(string message)
        {
            Console.WriteLine("   " + message);
        }

        /// <summary>Prints a compact live line of running totals (overwritten in place on a TTY).</summary>
        public void PrintLiveLine()
        {
            long enq;
            long svc;
            lock (_Lock)
            {
                enq = _TotalEnqueued;
                svc = _TotalServiced;
            }

            string line = $"   enqueued {enq,8:N0}   serviced {svc,8:N0}";
            if (!Console.IsOutputRedirected)
                Console.Write("\r" + line);
            else
                Console.WriteLine(line);
        }

        /// <summary>Prints the per-label summary table for the scenario just completed.</summary>
        public void PrintSummary()
        {
            List<string> labels = new List<string>();
            lock (_Lock)
            {
                HashSet<string> set = new HashSet<string>(StringComparer.Ordinal);
                foreach (string key in _Enqueued.Keys)
                    set.Add(key);
                foreach (string key in _Serviced.Keys)
                    set.Add(key);
                foreach (string key in set)
                    labels.Add(key);
            }

            labels.Sort(StringComparer.Ordinal);

            if (!Console.IsOutputRedirected)
                Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine($"   {"Label",-16}{"Enqueued",12}{"Serviced",12}{"Share",10}");
            Console.WriteLine($"   {new string('-', 16),-16}{new string('-', 10),12}{new string('-', 10),12}{new string('-', 8),10}");

            lock (_Lock)
            {
                foreach (string label in labels)
                {
                    _Enqueued.TryGetValue(label, out long e);
                    _Serviced.TryGetValue(label, out long s);
                    double share = _TotalServiced > 0 ? (double)s / _TotalServiced : 0.0;
                    Console.WriteLine($"   {label,-16}{e,12:N0}{s,12:N0}{share,9:P0} ");
                }

                Console.WriteLine($"   {new string('-', 16),-16}{new string('-', 10),12}{new string('-', 10),12}{new string('-', 8),10}");
                Console.WriteLine($"   {"TOTAL",-16}{_TotalEnqueued,12:N0}{_TotalServiced,12:N0}{1.0,9:P0} ");
            }

            Console.WriteLine();
        }
    }
}
