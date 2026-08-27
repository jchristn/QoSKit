namespace QoSKit
{
    using System;

    /// <summary>
    /// A token-bucket rate limiter used to police a low-latency priority class. Tokens accrue at a
    /// fixed rate up to a burst ceiling. Not thread-safe on its own; a queue calls it under its lock.
    /// </summary>
    public sealed class TokenBucket
    {
        private readonly double _RatePerSecond;
        private readonly double _Burst;
        private double _Tokens;
        private long _LastRefillMilliseconds;
        private bool _Initialized;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenBucket"/> class.
        /// </summary>
        /// <param name="ratePerSecond">Tokens accrued per second; must be greater than zero.</param>
        /// <param name="burst">The maximum token balance; must be zero or greater. The bucket starts full.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="ratePerSecond"/> is not positive, or <paramref name="burst"/> is negative.</exception>
        public TokenBucket(double ratePerSecond, double burst)
        {
            if (ratePerSecond <= 0)
                throw new ArgumentOutOfRangeException(nameof(ratePerSecond), "Rate must be greater than zero.");
            if (burst < 0)
                throw new ArgumentOutOfRangeException(nameof(burst), "Burst must be zero or greater.");
            _RatePerSecond = ratePerSecond;
            _Burst = burst;
            _Tokens = burst;
        }

        /// <summary>Gets the tokens accrued per second.</summary>
        public double RatePerSecond
        {
            get { return _RatePerSecond; }
        }

        /// <summary>Gets the maximum token balance.</summary>
        public double Burst
        {
            get { return _Burst; }
        }

        // Attempts to consume the given number of tokens, refilling first. Returns true if consumed.
        internal bool TryConsume(int tokens, IQoSTimeProvider time)
        {
            Refill(time);
            if (_Tokens >= tokens)
            {
                _Tokens -= tokens;
                return true;
            }

            return false;
        }

        // Returns true if the given number of tokens are available without consuming them.
        internal bool HasTokens(int tokens, IQoSTimeProvider time)
        {
            Refill(time);
            return _Tokens >= tokens;
        }

        private void Refill(IQoSTimeProvider time)
        {
            long now = time.MonotonicMilliseconds;
            if (!_Initialized)
            {
                _LastRefillMilliseconds = now;
                _Initialized = true;
                return;
            }

            long elapsed = now - _LastRefillMilliseconds;
            if (elapsed <= 0)
                return;
            _LastRefillMilliseconds = now;
            _Tokens = Math.Min(_Burst, _Tokens + (elapsed / 1000.0 * _RatePerSecond));
        }
    }
}
