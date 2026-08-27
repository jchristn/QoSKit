namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A low-latency queue (LLQ): one or more strict-priority classes served ahead of a set of
    /// weighted-fair classes. Each priority class may carry a token-bucket policer; a null policer is
    /// unpoliced strict priority (which can starve fair classes). Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class LowLatencyQoSQueue<T> : QoSQueueBase<T>
    {
        /// <summary>The reserved name of the implicit default fair class.</summary>
        public const string DefaultClassName = "class-default";

        private readonly List<LlqPriorityState<T>> _PriorityClasses = new List<LlqPriorityState<T>>();
        private readonly List<WfqClassState<T>> _FairClasses = new List<WfqClassState<T>>();
        private double _VirtualTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="LowLatencyQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="priorityClasses">The strict-priority classes, served in declared order.</param>
        /// <param name="fairClasses">The weighted-fair classes served after the priority classes.</param>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <param name="costSelector">Maps an item to a cost (default 1); also the policer's token cost.</param>
        /// <param name="defaultClassWeight">The weight of the implicit default fair class. Default 1, minimum 1.</param>
        /// <exception cref="ArgumentNullException"><paramref name="priorityClasses"/> or <paramref name="fairClasses"/> is null.</exception>
        /// <exception cref="ArgumentException">A duplicate class name is present.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="defaultClassWeight"/> is less than 1.</exception>
        public LowLatencyQoSQueue(
            IEnumerable<TrafficClass<T>> priorityClasses,
            IEnumerable<TrafficClass<T>> fairClasses,
            QoSQueueOptions? options = null,
            Func<T, int>? costSelector = null,
            int defaultClassWeight = 1)
            : base(options, "llq")
        {
            if (priorityClasses == null)
                throw new ArgumentNullException(nameof(priorityClasses));
            if (fairClasses == null)
                throw new ArgumentNullException(nameof(fairClasses));
            if (defaultClassWeight < 1)
                throw new ArgumentOutOfRangeException(nameof(defaultClassWeight), "Default class weight must be at least 1.");

            HashSet<string> names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (TrafficClass<T> tc in priorityClasses)
            {
                if (!names.Add(tc.Name))
                    throw new ArgumentException($"Duplicate class name '{tc.Name}'.", nameof(priorityClasses));
                _PriorityClasses.Add(new LlqPriorityState<T>(tc.Name, tc.Matcher, tc.RateLimit));
            }

            bool defaultProvided = false;
            foreach (TrafficClass<T> tc in fairClasses)
            {
                if (!names.Add(tc.Name))
                    throw new ArgumentException($"Duplicate class name '{tc.Name}'.", nameof(fairClasses));
                _FairClasses.Add(new WfqClassState<T>(tc.Name, tc.Matcher, tc.Weight));
                if (string.Equals(tc.Name, DefaultClassName, StringComparison.OrdinalIgnoreCase))
                    defaultProvided = true;
            }

            if (!defaultProvided)
                _FairClasses.Add(new WfqClassState<T>(DefaultClassName, _ => true, defaultClassWeight));

            SetCostSelector(costSelector);
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
            for (int i = 0; i < _PriorityClasses.Count; i++)
            {
                if (_PriorityClasses[i].Matcher(entry.Item))
                {
                    entry.Band = i;
                    entry.Key = _PriorityClasses[i].Name;
                    return;
                }
            }

            for (int j = 0; j < _FairClasses.Count; j++)
            {
                if (_FairClasses[j].Matcher(entry.Item))
                {
                    entry.Band = _PriorityClasses.Count + j;
                    entry.Key = _FairClasses[j].Name;
                    return;
                }
            }

            entry.Band = _PriorityClasses.Count + _FairClasses.Count - 1;
            entry.Key = _FairClasses[_FairClasses.Count - 1].Name;
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            if (entry.Band < _PriorityClasses.Count)
            {
                _PriorityClasses[entry.Band].Queue.AddLast(entry);
            }
            else
            {
                WfqClassState<T> cls = _FairClasses[entry.Band - _PriorityClasses.Count];
                double start = Math.Max(_VirtualTime, cls.LastFinish);
                entry.VirtualFinish = start + entry.Cost / cls.Weight;
                cls.LastFinish = entry.VirtualFinish;
                cls.Queue.AddLast(entry);
            }
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            for (int i = 0; i < _PriorityClasses.Count; i++)
            {
                LlqPriorityState<T> pc = _PriorityClasses[i];
                if (pc.Queue.First != null)
                {
                    QoSEntry<T> head = pc.Queue.First.Value;
                    if (pc.RateLimit == null || pc.RateLimit.TryConsume(head.Cost, TimeProvider))
                    {
                        entry = head;
                        pc.Queue.RemoveFirst();
                        return true;
                    }
                }
            }

            WfqClassState<T>? best = SelectMinFinishFair();
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
            for (int i = 0; i < _PriorityClasses.Count; i++)
            {
                LlqPriorityState<T> pc = _PriorityClasses[i];
                if (pc.Queue.First != null)
                {
                    QoSEntry<T> head = pc.Queue.First.Value;
                    if (pc.RateLimit == null || pc.RateLimit.HasTokens(head.Cost, TimeProvider))
                    {
                        entry = head;
                        return true;
                    }
                }
            }

            WfqClassState<T>? best = SelectMinFinishFair();
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
            LinkedList<QoSEntry<T>>? oldestList = null;
            long oldestSeq = long.MaxValue;
            foreach (LlqPriorityState<T> pc in _PriorityClasses)
                ConsiderOldest(pc.Queue, ref oldestList, ref oldestSeq);
            foreach (WfqClassState<T> cls in _FairClasses)
                ConsiderOldest(cls.Queue, ref oldestList, ref oldestSeq);

            if (oldestList != null)
            {
                entry = oldestList.First!.Value;
                oldestList.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            foreach (LlqPriorityState<T> pc in _PriorityClasses)
                pc.Queue.Clear();
            foreach (WfqClassState<T> cls in _FairClasses)
            {
                cls.Queue.Clear();
                cls.LastFinish = 0;
            }

            _VirtualTime = 0;
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            foreach (LlqPriorityState<T> pc in _PriorityClasses)
            {
                for (LinkedListNode<QoSEntry<T>>? node = pc.Queue.First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }

            foreach (WfqClassState<T> cls in _FairClasses)
            {
                for (LinkedListNode<QoSEntry<T>>? node = cls.Queue.First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }
        }

        private static void ConsiderOldest(LinkedList<QoSEntry<T>> queue, ref LinkedList<QoSEntry<T>>? oldestList, ref long oldestSeq)
        {
            if (queue.First != null && queue.First.Value.Sequence < oldestSeq)
            {
                oldestSeq = queue.First.Value.Sequence;
                oldestList = queue;
            }
        }

        private WfqClassState<T>? SelectMinFinishFair()
        {
            WfqClassState<T>? best = null;
            double bestFinish = double.MaxValue;
            foreach (WfqClassState<T> cls in _FairClasses)
            {
                if (cls.Queue.First != null && cls.Queue.First.Value.VirtualFinish < bestFinish)
                {
                    bestFinish = cls.Queue.First.Value.VirtualFinish;
                    best = cls;
                }
            }

            return best;
        }
    }
}
