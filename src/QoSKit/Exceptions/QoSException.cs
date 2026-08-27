namespace QoSKit
{
    using System;

    /// <summary>
    /// The abstract base type for every QoSKit-specific exception. Catch this to handle any
    /// QoSKit domain failure, or catch a concrete derived type for precise handling.
    /// </summary>
    public abstract class QoSException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QoSException"/> class with a specified message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        protected QoSException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSException"/> class with a specified message
        /// and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of this exception.</param>
        protected QoSException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
