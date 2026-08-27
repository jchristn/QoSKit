namespace QoSKit
{
    /// <summary>
    /// Thrown by <c>Enqueue</c> on a router when no route matches the item and no default route is
    /// configured. The non-throwing <c>TryEnqueue</c> returns <c>false</c> instead.
    /// </summary>
    public sealed class UnroutableItemException : QoSException
    {
        /// <summary>
        /// The name of the router that could not route the item. Never null.
        /// </summary>
        public string RouterName { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnroutableItemException"/> class.
        /// </summary>
        /// <param name="routerName">The name of the router.</param>
        public UnroutableItemException(string routerName)
            : base($"Router '{routerName}' has no matching route and no default route for the item.")
        {
            RouterName = routerName;
        }
    }
}
