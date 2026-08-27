namespace QoSKit
{
    /// <summary>
    /// Serializes and deserializes queue payloads for persistence. Required because the library
    /// cannot serialize an arbitrary payload type on its own.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public interface IQoSSerializer<T>
    {
        /// <summary>Serializes an item to bytes.</summary>
        /// <param name="item">The item to serialize.</param>
        /// <returns>The serialized bytes.</returns>
        byte[] Serialize(T item);

        /// <summary>Deserializes an item from bytes.</summary>
        /// <param name="data">The serialized bytes.</param>
        /// <returns>The deserialized item.</returns>
        T Deserialize(byte[] data);
    }
}
