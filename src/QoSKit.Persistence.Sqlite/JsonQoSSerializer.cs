namespace QoSKit.Persistence.Sqlite
{
    using System.Text.Json;
    using QoSKit;

    /// <summary>
    /// A convenience <see cref="IQoSSerializer{T}"/> backed by <see cref="System.Text.Json"/>. Supply
    /// your own serializer for hotter paths or non-JSON formats.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    public sealed class JsonQoSSerializer<T> : IQoSSerializer<T>
    {
        private readonly JsonSerializerOptions? _Options;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonQoSSerializer{T}"/> class.
        /// </summary>
        /// <param name="options">Optional JSON options; when null, defaults are used.</param>
        public JsonQoSSerializer(JsonSerializerOptions? options = null)
        {
            _Options = options;
        }

        /// <inheritdoc/>
        public byte[] Serialize(T item)
        {
            return JsonSerializer.SerializeToUtf8Bytes(item, _Options);
        }

        /// <inheritdoc/>
        public T Deserialize(byte[] data)
        {
            T? result = JsonSerializer.Deserialize<T>(data, _Options);
            return result!;
        }
    }
}
