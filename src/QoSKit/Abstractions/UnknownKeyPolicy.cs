namespace QoSKit
{
    /// <summary>
    /// Determines how a keyed scheduling queue handles a classification key that is not in its
    /// defined set of keys (a typo, a case mismatch under an ordinal comparer, a null/empty key, or
    /// a value that was never registered).
    /// </summary>
    public enum UnknownKeyPolicy
    {
        /// <summary>
        /// Admit the key as a new flow or sub-queue at the configured default weight, created on
        /// first sighting. The natural model for weighted fair queuing over an open key space.
        /// </summary>
        CreateDynamic = 0,

        /// <summary>
        /// Route the item to the flow or sub-queue designated as the default at construction.
        /// Requires a default to exist.
        /// </summary>
        RouteToDefault = 1,

        /// <summary>
        /// Do not admit the item. <c>TryEnqueue</c> returns <c>false</c> and <c>Enqueue</c> throws
        /// <see cref="UnknownClassificationException"/>; an <c>ItemDropped</c> event fires.
        /// </summary>
        Reject = 2,

        /// <summary>
        /// Always raise <see cref="UnknownClassificationException"/>, including from <c>TryEnqueue</c>,
        /// for callers that treat an unknown key as a hard programming error.
        /// </summary>
        Throw = 3
    }
}
