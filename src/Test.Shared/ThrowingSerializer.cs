namespace Test.Shared
{
    using System;
    using QoSKit;

    /// <summary>
    /// A serializer that always fails, used to verify that a serialization error aborts an enqueue
    /// before the item is admitted.
    /// </summary>
    public sealed class ThrowingSerializer : IQoSSerializer<DemoItem>
    {
        /// <inheritdoc/>
        public byte[] Serialize(DemoItem item)
        {
            throw new InvalidOperationException("Serialization failed for test.");
        }

        /// <inheritdoc/>
        public DemoItem Deserialize(byte[] data)
        {
            throw new InvalidOperationException("Deserialization failed for test.");
        }
    }
}
