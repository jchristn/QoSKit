namespace QoSKit
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    // A pending asynchronous waiter (for an item to dequeue, or for space to enqueue under a Block
    // policy). Continuations run asynchronously so a signal raised while holding the queue lock never
    // executes user continuation code under that lock.
    internal sealed class QoSWaiter
    {
        internal readonly TaskCompletionSource<bool> Completion =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal LinkedListNode<QoSWaiter>? Node;

        internal CancellationTokenRegistration Registration;
    }
}
