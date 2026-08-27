namespace QoSKit
{
    /// <summary>
    /// Identifies why an item was dropped rather than serviced.
    /// </summary>
    public enum DropReason
    {
        /// <summary>
        /// The incoming (newest) item was discarded because the queue was full under
        /// <see cref="OverflowPolicy.DropNewest"/>.
        /// </summary>
        Newest = 0,

        /// <summary>
        /// The oldest item was evicted to make room for a newer one under
        /// <see cref="OverflowPolicy.DropOldest"/>.
        /// </summary>
        Oldest = 1,

        /// <summary>
        /// The item's classification key was not recognized and the policy was
        /// <see cref="UnknownKeyPolicy.Reject"/>.
        /// </summary>
        UnknownClass = 2,

        /// <summary>
        /// A router had no matching route and no default route for the item.
        /// </summary>
        Unroutable = 3
    }
}
