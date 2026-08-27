namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A weighted fair queue (WFQ). Approximates generalized processor sharing across flows using a
    /// virtual-time finish tag per item. Flow keys form an open namespace; unrecognized keys are
    /// handled per the configured <see cref="UnknownKeyPolicy"/>. Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class WeightedFairQoSQueue<T> : QoSQueueBase<T>
    {
        private readonly Func<T, string> _FlowSelector;
        private readonly Dictionary<string, WfqFlowState<T>> _Flows;
        private readonly UnknownKeyPolicy _UnknownKeyPolicy;
        private readonly double _DefaultWeight;
        private readonly string? _DefaultKey;
        private double _VirtualTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="WeightedFairQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="flowSelector">Maps an item to a flow key.</param>
        /// <param name="flows">The initially defined flows and weights.</param>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <param name="costSelector">Maps an item to a cost (default 1 for packet-count fairness).</param>
        /// <param name="unknownKeyPolicy">How to handle a key not in the defined set. Default is <see cref="UnknownKeyPolicy.CreateDynamic"/>.</param>
        /// <param name="defaultWeight">The weight assigned to dynamically created flows. Default 1, minimum 1.</param>
        /// <param name="defaultKey">The flow key used under <see cref="UnknownKeyPolicy.RouteToDefault"/>.</param>
        /// <param name="keyComparer">Comparer for flow keys. Default is <see cref="StringComparer.OrdinalIgnoreCase"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="flowSelector"/> or <paramref name="flows"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="defaultWeight"/> is less than 1.</exception>
        /// <exception cref="ArgumentException">A duplicate flow key, or a route-to-default policy without a valid default key.</exception>
        public WeightedFairQoSQueue(
            Func<T, string> flowSelector,
            IEnumerable<WeightedFlow> flows,
            QoSQueueOptions? options = null,
            Func<T, int>? costSelector = null,
            UnknownKeyPolicy unknownKeyPolicy = UnknownKeyPolicy.CreateDynamic,
            int defaultWeight = 1,
            string? defaultKey = null,
            StringComparer? keyComparer = null)
            : base(options, "wfq")
        {
            _FlowSelector = flowSelector ?? throw new ArgumentNullException(nameof(flowSelector));
            if (flows == null)
                throw new ArgumentNullException(nameof(flows));
            if (defaultWeight < 1)
                throw new ArgumentOutOfRangeException(nameof(defaultWeight), "Default weight must be at least 1.");

            StringComparer comparer = keyComparer ?? StringComparer.OrdinalIgnoreCase;
            _Flows = new Dictionary<string, WfqFlowState<T>>(comparer);
            foreach (WeightedFlow flow in flows)
            {
                if (_Flows.ContainsKey(flow.Name))
                    throw new ArgumentException($"Duplicate flow key '{flow.Name}'.", nameof(flows));
                _Flows[flow.Name] = new WfqFlowState<T>(flow.Weight);
            }

            _UnknownKeyPolicy = unknownKeyPolicy;
            _DefaultWeight = defaultWeight;
            _DefaultKey = defaultKey;

            if (unknownKeyPolicy == UnknownKeyPolicy.RouteToDefault && (defaultKey == null || !_Flows.ContainsKey(defaultKey)))
                throw new ArgumentException("UnknownKeyPolicy.RouteToDefault requires a defaultKey that names a defined flow.", nameof(defaultKey));

            SetCostSelector(costSelector);
        }

        /// <summary>Enqueues an item into a specific flow, bypassing the selector.</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="flowKey">The flow key.</param>
        public void Enqueue(T item, string flowKey)
        {
            EnqueueInternal(item, flowKey, throwOnFailure: true);
        }

        /// <summary>Attempts to enqueue an item into a specific flow, bypassing the selector.</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="flowKey">The flow key.</param>
        /// <returns><c>true</c> if admitted; otherwise <c>false</c>.</returns>
        public bool TryEnqueue(T item, string flowKey)
        {
            return EnqueueInternal(item, flowKey, throwOnFailure: false);
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
            entry.Key = entry.Override is string key ? key : _FlowSelector(entry.Item);
        }

        /// <inheritdoc/>
        private protected override bool ResolveClassification(QoSEntry<T> entry)
        {
            string? key = entry.Key;
            if (key != null && _Flows.ContainsKey(key))
                return true;

            switch (_UnknownKeyPolicy)
            {
                case UnknownKeyPolicy.CreateDynamic:
                    string dynamicKey = key ?? string.Empty;
                    if (!_Flows.ContainsKey(dynamicKey))
                        _Flows[dynamicKey] = new WfqFlowState<T>(_DefaultWeight);
                    entry.Key = dynamicKey;
                    return true;

                case UnknownKeyPolicy.RouteToDefault:
                    entry.Key = _DefaultKey;
                    return true;

                case UnknownKeyPolicy.Reject:
                    return false;

                default:
                    throw new UnknownClassificationException(Name, key);
            }
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            WfqFlowState<T> flow = _Flows[entry.Key!];
            double start = Math.Max(_VirtualTime, flow.LastFinish);
            entry.VirtualFinish = start + entry.Cost / flow.Weight;
            flow.LastFinish = entry.VirtualFinish;
            flow.Queue.AddLast(entry);
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            WfqFlowState<T>? best = SelectMinFinishFlow();
            if (best != null)
            {
                entry = best.Queue.First!.Value;
                best.Queue.RemoveFirst();
                _VirtualTime = entry.VirtualFinish;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryPeek(out QoSEntry<T> entry)
        {
            WfqFlowState<T>? best = SelectMinFinishFlow();
            if (best != null)
            {
                entry = best.Queue.First!.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            WfqFlowState<T>? oldest = null;
            long oldestSeq = long.MaxValue;
            foreach (WfqFlowState<T> flow in _Flows.Values)
            {
                if (flow.Queue.First != null && flow.Queue.First.Value.Sequence < oldestSeq)
                {
                    oldestSeq = flow.Queue.First.Value.Sequence;
                    oldest = flow;
                }
            }

            if (oldest != null)
            {
                entry = oldest.Queue.First!.Value;
                oldest.Queue.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            foreach (WfqFlowState<T> flow in _Flows.Values)
            {
                flow.Queue.Clear();
                flow.LastFinish = 0;
            }

            _VirtualTime = 0;
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            foreach (WfqFlowState<T> flow in _Flows.Values)
            {
                for (LinkedListNode<QoSEntry<T>>? node = flow.Queue.First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }
        }

        private WfqFlowState<T>? SelectMinFinishFlow()
        {
            WfqFlowState<T>? best = null;
            double bestFinish = double.MaxValue;
            foreach (WfqFlowState<T> flow in _Flows.Values)
            {
                if (flow.Queue.First != null)
                {
                    double finish = flow.Queue.First.Value.VirtualFinish;
                    if (finish < bestFinish)
                    {
                        bestFinish = finish;
                        best = flow;
                    }
                }
            }

            return best;
        }
    }
}
