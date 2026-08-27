namespace Test.Shared
{
    using System;

    /// <summary>
    /// Resolves the soak duration from the <c>QOSKIT_SOAK_SECONDS</c> environment variable. Defaults
    /// to a short window for the PR gate; a nightly job sets it higher for long-form endurance.
    /// </summary>
    public static class SoakConfig
    {
        /// <summary>The soak duration in seconds. Default 3; overridden by <c>QOSKIT_SOAK_SECONDS</c>.</summary>
        public static int Seconds
        {
            get
            {
                string? value = Environment.GetEnvironmentVariable("QOSKIT_SOAK_SECONDS");
                if (value != null && int.TryParse(value, out int seconds) && seconds > 0)
                    return seconds;
                return 3;
            }
        }
    }
}
