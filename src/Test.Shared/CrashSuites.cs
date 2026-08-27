namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using QoSKit.Persistence.Sqlite;
    using Touchstone.Core;

    /// <summary>
    /// Persistence durability under a real process crash (plan Suite T, T16). Spawns a child that
    /// durably enqueues, hard-kills it mid-flight, and verifies every acknowledged item survived.
    /// Skipped when the crash-harness build output cannot be located.
    /// </summary>
    public static class CrashSuites
    {
        /// <summary>The crash-durability suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();
            string? harness = LocateHarness();

            if (harness == null)
            {
                cases.Add(new TestCaseDescriptor(
                    suiteId: "Crash",
                    caseId: "NoLossOnKill",
                    displayName: "Durable enqueues survive a hard process kill",
                    skip: true,
                    skipReason: "Test.CrashHarness build output not found.",
                    executeAsync: _ => Task.CompletedTask));
            }
            else
            {
                cases.Add(new TestCaseDescriptor(
                    suiteId: "Crash",
                    caseId: "NoLossOnKill",
                    displayName: "Durable enqueues survive a hard process kill",
                    executeAsync: ct => RunAsync(harness)));
            }

            return new TestSuiteDescriptor("Crash", "Crash Durability", cases);
        }

        private static async Task RunAsync(string harnessDll)
        {
            string path = Path.Combine(Path.GetTempPath(), "qoskit-crash-" + Guid.NewGuid().ToString("N") + ".db");
            const int threshold = 300;

            try
            {
                ProcessStartInfo psi = new ProcessStartInfo
                {
                    FileName = "dotnet",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                psi.ArgumentList.Add(harnessDll);
                psi.ArgumentList.Add(path);

                using Process process = new Process { StartInfo = psi };
                process.Start();

                int lastCommitted = 0;
                DateTime deadline = DateTime.UtcNow.AddSeconds(30);
                while (lastCommitted < threshold && DateTime.UtcNow < deadline)
                {
                    string? line = await process.StandardOutput.ReadLineAsync().ConfigureAwait(false);
                    if (line == null)
                        break;
                    if (line.StartsWith("COMMITTED ", StringComparison.Ordinal)
                        && int.TryParse(line.Substring("COMMITTED ".Length), out int n))
                    {
                        lastCommitted = n;
                    }
                }

                Check.True(lastCommitted >= threshold, $"child committed at least {threshold} items (saw {lastCommitted})");

                // Hard kill — no graceful shutdown, no WAL checkpoint. Simulates a crash.
                process.Kill(entireProcessTree: true);
                process.WaitForExit(10000);

                // Reopen and confirm every acknowledged item survived and is in order.
                using SqliteQoSStore<int> store = new SqliteQoSStore<int>(QoSPersistenceOptions<int>.Fast(path));
                IReadOnlyList<QoSPersistedItem<int>> recovered = store.Recover();

                Check.True(recovered.Count >= lastCommitted, $"recovered {recovered.Count} >= committed {lastCommitted} (no acknowledged loss)");
                for (int i = 0; i < lastCommitted; i++)
                    Check.Equal(i, recovered[i].Item, "recovered item in order at " + i);
            }
            finally
            {
                TryDelete(path);
                TryDelete(path + "-wal");
                TryDelete(path + "-shm");
            }
        }

        private static string? LocateHarness()
        {
            try
            {
                DirectoryInfo? baseDir = new DirectoryInfo(AppContext.BaseDirectory);
                string tfm = baseDir.Name;                                  // e.g. net10.0
                string config = baseDir.Parent?.Name ?? "Debug";           // e.g. Debug
                DirectoryInfo? srcDir = baseDir.Parent?.Parent?.Parent?.Parent; // .../src

                if (srcDir == null)
                    return null;

                string candidate = Path.Combine(srcDir.FullName, "Test.CrashHarness", "bin", config, tfm, "Test.CrashHarness.dll");
                return File.Exists(candidate) ? candidate : null;
            }
            catch (IOException)
            {
                return null;
            }
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
            catch (IOException)
            {
            }
        }
    }
}
