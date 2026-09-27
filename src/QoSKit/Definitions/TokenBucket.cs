namespace QoSKit
{
    using System;

    /// <summary>
    /// A token-bucket rate limiter used to police a low-latency priority class. Tokens accrue at a
    /// fixed rate up to a burst ceiling. An item conforms when the balance covers its cost; an item
    /// whose cost exceeds the burst conforms once the bucket is full and leaves a negative balance
    /// (a debt repaid by refill), so it is rate-limited rather than held back forever. Not
    /// thread-safe on its own; a queue calls it under its lock.
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
        // An item costing more than the burst conforms once the bucket is full and drives the balance
        // negative (a debt repaid by later refill), so an oversize item is rate-limited rather than
        // stranded forever behind a ceiling it can never reach.
        internal bool TryConsume(int tokens, IQoSTimeProvider time)
        {
            Refill(time);
            if (Conforms(tokens))
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
            return Conforms(tokens);
        }

        // Unconditionally charges the given number of tokens, allowing the balance to go negative.
        // Used when an item is removed outside the normal scheduling decision (a chain pump taking the
        // exact item it already forwarded) so the class's long-run rate stays honest. Returns true if
        // the charge conformed (the tokens were available).
        internal bool Charge(int tokens, IQoSTimeProvider time)
        {
            Refill(time);
            bool conformed = Conforms(tokens);
            _Tokens -= tokens;
            return conformed;
        }

        // Returns the milliseconds until the given number of tokens will conform, or zero if they
        // already do. Rounds up so a waiter armed with this delay finds the tokens present.
        internal long MillisecondsUntilConforming(int tokens, IQoSTimeProvider time)
        {
            Refill(time);
            if (Conforms(tokens))
                return 0;
            double needed = Math.Min(tokens, _Burst) - _Tokens;
            double milliseconds = Math.Ceiling(needed * 1000.0 / _RatePerSecond);
            if (milliseconds >= long.MaxValue)
                return long.MaxValue;
            return milliseconds < 1 ? 1 : (long)milliseconds;
        }

        private bool Conforms(int tokens)
        {
            return _Tokens >= Math.Min(tokens, _Burst);
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
