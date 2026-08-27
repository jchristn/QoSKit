namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Minimal assertion helpers. A failed assertion throws, which Touchstone records as a failure.
    /// </summary>
    public static class Check
    {
        /// <summary>Asserts a condition is true.</summary>
        /// <param name="condition">The condition.</param>
        /// <param name="message">The failure message.</param>
        public static void True(bool condition, string message)
        {
            if (!condition)
                throw new InvalidOperationException("Assertion failed: " + message);
        }

        /// <summary>Asserts a condition is false.</summary>
        /// <param name="condition">The condition.</param>
        /// <param name="message">The failure message.</param>
        public static void False(bool condition, string message)
        {
            True(!condition, message);
        }

        /// <summary>Asserts two values are equal.</summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="expected">The expected value.</param>
        /// <param name="actual">The actual value.</param>
        /// <param name="message">The failure message.</param>
        public static void Equal<T>(T expected, T actual, string message)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
                throw new InvalidOperationException($"Assertion failed: {message}. Expected '{expected}', got '{actual}'.");
        }

        /// <summary>Asserts that an action throws an exception of a given type.</summary>
        /// <typeparam name="TException">The expected exception type.</typeparam>
        /// <param name="action">The action.</param>
        /// <param name="message">The failure message.</param>
        public static void Throws<TException>(Action action, string message)
            where TException : Exception
        {
            try
            {
                action();
            }
            catch (TException)
            {
                return;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Assertion failed: {message}. Expected {typeof(TException).Name}, got {ex.GetType().Name}.");
            }

            throw new InvalidOperationException($"Assertion failed: {message}. Expected {typeof(TException).Name}, but nothing was thrown.");
        }

        /// <summary>Asserts that an asynchronous action throws an exception of a given type.</summary>
        /// <typeparam name="TException">The expected exception type.</typeparam>
        /// <param name="action">The asynchronous action.</param>
        /// <param name="message">The failure message.</param>
        /// <returns>A task that completes when the assertion is verified.</returns>
        public static async Task ThrowsAsync<TException>(Func<Task> action, string message)
            where TException : Exception
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (TException)
            {
                return;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Assertion failed: {message}. Expected {typeof(TException).Name}, got {ex.GetType().Name}.");
            }

            throw new InvalidOperationException($"Assertion failed: {message}. Expected {typeof(TException).Name}, but nothing was thrown.");
        }
    }
}
