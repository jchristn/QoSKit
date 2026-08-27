namespace Test.Shared
{
    /// <summary>
    /// A small payload used across the test suites. Carries the fields the disciplines classify on.
    /// </summary>
    public sealed class DemoItem
    {
        /// <summary>A unique identifier used to assert conservation and ordering.</summary>
        public int Id { get; set; }

        /// <summary>A priority band (lower is higher priority).</summary>
        public int Priority { get; set; }

        /// <summary>A flow or class key for keyed disciplines.</summary>
        public string Flow { get; set; } = "default";

        /// <summary>A cost/size for size-fair disciplines.</summary>
        public int Cost { get; set; } = 1;

        /// <summary>An arbitrary tier label used by class matchers.</summary>
        public string Tier { get; set; } = "bronze";

        /// <summary>Initializes a new instance of the <see cref="DemoItem"/> class.</summary>
        public DemoItem()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DemoItem"/> class with an id.</summary>
        /// <param name="id">The identifier.</param>
        public DemoItem(int id)
        {
            Id = id;
        }
    }
}
