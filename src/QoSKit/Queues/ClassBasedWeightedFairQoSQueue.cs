namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A class-based weighted fair queue (CBWFQ). Items are matched to named traffic classes by
    /// predicate (first match wins); classes are served in weighted-fair proportion. An implicit,
    /// non-removable <c>class-default</c> catches unmatched items. Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class ClassBasedWeightedFairQoSQueue<T> : QoSQueueBase<T>
    {
        /// <summary>The reserved name of the implicit default class.</summary>
        public const string DefaultClassName = "class-default";

        private readonly List<WfqClassState<T>> _Classes = new List<WfqClassState<T>>();
        private double _VirtualTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClassBasedWeightedFairQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="classes">The traffic classes, evaluated in order (first match wins). May be empty.</param>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <param name="costSelector">Maps an item to a cost (default 1).</param>
        /// <param name="defaultClassWeight">The weight of the implicit default class. Default 1, minimum 1.</param>
        /// <exception cref="ArgumentNullException"><paramref name="classes"/> is null.</exception>
        /// <exception cref="ArgumentException">A duplicate class name is present.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="defaultClassWeight"/> is less than 1.</exception>
        public ClassBasedWeightedFairQoSQueue(
            IEnumerable<TrafficClass<T>> classes,
            QoSQueueOptions? options = null,
            Func<T, int>? costSelector = null,
            int defaultClassWeight = 1)
            : base(options, "cbwfq")
        {
            if (classes == null)
                throw new ArgumentNullException(nameof(classes));
            if (defaultClassWeight < 1)
                throw new ArgumentOutOfRangeException(nameof(defaultClassWeight), "Default class weight must be at least 1.");

            HashSet<string> names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            bool defaultProvided = false;
            foreach (TrafficClass<T> tc in classes)
            {
                if (!names.Add(tc.Name))
                    throw new ArgumentException($"Duplicate class name '{tc.Name}'.", nameof(classes));
                _Classes.Add(new WfqClassState<T>(tc.Name, tc.Matcher, tc.Weight));
                if (string.Equals(tc.Name, DefaultClassName, StringComparison.OrdinalIgnoreCase))
                    defaultProvided = true;
            }

            if (!defaultProvided)
                _Classes.Add(new WfqClassState<T>(DefaultClassName, _ => true, defaultClassWeight));

            SetCostSelector(costSelector);
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
            for (int i = 0; i < _Classes.Count; i++)
            {
                if (_Classes[i].Matcher(entry.Item))
                {
                    entry.Band = i;
                    entry.Key = _Classes[i].Name;
                    return;
                }
            }

            // The implicit default (or a provided class-default) always matches; fall back to the last.
            entry.Band = _Classes.Count - 1;
            entry.Key = _Classes[_Classes.Count - 1].Name;
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            WfqClassState<T> cls = _Classes[entry.Band];
            double start = Math.Max(_VirtualTime, cls.LastFinish);
            entry.VirtualFinish = start + entry.Cost / cls.Weight;
            cls.LastFinish = entry.VirtualFinish;
            entry.Node = cls.Queue.AddLast(entry);
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            WfqClassState<T>? best = SelectMinFinishClass();
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
            WfqClassState<T>? best = SelectMinFinishClass();
            if (best != null)
            {
                entry = best.Queue.First!.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreRemoveEntry(QoSEntry<T> entry)
        {
            if (!base.StoreRemoveEntry(entry))
                return false;
            _VirtualTime = entry.VirtualFinish;
            return true;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            WfqClassState<T>? oldest = null;
            long oldestSeq = long.MaxValue;
            foreach (WfqClassState<T> cls in _Classes)
            {
                if (cls.Queue.First != null && cls.Queue.First.Value.Sequence < oldestSeq)
                {
                    oldestSeq = cls.Queue.First.Value.Sequence;
                    oldest = cls;
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
            foreach (WfqClassState<T> cls in _Classes)
            {
                cls.Queue.Clear();
                cls.LastFinish = 0;
            }

            _VirtualTime = 0;
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            foreach (WfqClassState<T> cls in _Classes)
            {
                for (LinkedListNode<QoSEntry<T>>? node = cls.Queue.First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }
        }

        private WfqClassState<T>? SelectMinFinishClass()
        {
            WfqClassState<T>? best = null;
            double bestFinish = double.MaxValue;
            foreach (WfqClassState<T> cls in _Classes)
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
