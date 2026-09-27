namespace QoSKit
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    // A pending asynchronous waiter (for an item to dequeue, or for space to enqueue under a Block
    // policy). Continuations run asynchronously so a signal raised while holding the queue lock never
    // executes user continuation code under that lock. Registration and Timer are owned by the task
    // that registered the waiter: only that task assigns and disposes them, so no other thread ever
    // reads the multi-field registration struct while it is being written.
    internal sealed class QoSWaiter
    {
        internal readonly TaskCompletionSource<bool> Completion =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal LinkedListNode<QoSWaiter>? Node;

        internal CancellationTokenRegistration Registration;

        // Armed only when items are resident but none is currently eligible (for example an LLQ
        // priority class throttled by its policer); fires when the earliest item can become eligible.
        internal Timer? Timer;
    }
}
