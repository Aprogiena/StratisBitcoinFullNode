using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Stratis.Bitcoin.Utilities
{
    /// <summary>
    /// Async queue is a simple thread-safe queue that asynchronously executes 
    /// a user-defined callback when a new item is added to the queue.
    /// <para>
    /// Additionally, it allows manual trigger of a second user-defined callback.
    /// </para>
    /// </summary>
    /// <typeparam name="T">Type of items to be inserted in the queue.</typeparam>
    /// <remarks>
    /// The queue guarantees that the user-defined callbacks are executed only once at the time. 
    /// If an item is added to the queue while the callback is being executed, the on-enqueue callback will be called 
    /// after the current execution is finished. If a manual signal is triggered while the on-enqueue callback
    /// is being executed, it on-signal callback will be executed after the current execution is finished.
    /// If a manual signal is triggered while the on-signal callback is being executed, it will not be executed 
    /// for the second time.
    /// </remarks>
    public class AsyncQueue<T>: IDisposable
    {
        /// <summary>
        /// Represents a callback method to be executed when a new item is added to the queue.
        /// </summary>
        /// <param name="item">Newly added item.</param>
        /// <param name="cancellationToken">Cancellation token that the callback method should use for its async operations to avoid blocking the queue during shutdown.</param>
        public delegate Task OnEnqueueAsync(T item, CancellationToken cancellationToken);

        /// <summary>
        /// Represents a callback method to be executed when <see cref="Signal"/> is called.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token that the callback method should use for its async operations to avoid blocking the queue during shutdown.</param>
        public delegate Task OnSignalAsync(CancellationToken cancellationToken);

        /// <summary>Storage of items in the queue that are waiting to be consumed.</summary>
        private readonly ConcurrentQueue<T> items;

        /// <summary>Event that is triggered when at least one new item is waiting in the queue.</summary>
        private readonly AsyncManualResetEvent enqueuedSignal;

        /// <summary>Event that is triggered when <see cref="Signal"/> is called.</summary>
        private readonly AsyncManualResetEvent manualSignal;

        /// <summary>Callback routine to be called when a new item is added to the queue.</summary>
        private readonly OnEnqueueAsync onEnqueueAsync;

        /// <summary>Callback routine to be called when <see cref="Signal"/> is called, or <c>null</c> if this callback is not used.</summary>
        private readonly OnSignalAsync onSignalAsync;

        /// <summary>Consumer of the items in the queue which responsibility is to execute the user defined callback.</summary>
        private readonly Task consumerTask;

        /// <summary>Cancellation that is triggered when the component is disposed.</summary>
        private readonly CancellationTokenSource cancellationTokenSource;

        /// <summary>
        /// Initializes the queue.
        /// </summary>
        /// <param name="onEnqueueAsync">Callback routine to be called when a new item is added to the queue.</param>
        /// <param name="onSignalAsync">Callback routine to be called when <see cref="Signal"/> is called, or <c>null</c> if this callback is not used.</param>
        public AsyncQueue(OnEnqueueAsync onEnqueueAsync, OnSignalAsync onSignalAsync = null)
        {
            Guard.NotNull(onEnqueueAsync, nameof(onEnqueueAsync));

            this.items = new ConcurrentQueue<T>();
            this.enqueuedSignal = new AsyncManualResetEvent();
            this.manualSignal = new AsyncManualResetEvent();

            this.onEnqueueAsync = onEnqueueAsync;
            this.onSignalAsync = onSignalAsync;
            this.cancellationTokenSource = new CancellationTokenSource();
            this.consumerTask = ConsumerAsync();
        }

        /// <summary>
        /// Consumer of the newly added items to the queue that waits for the signal 
        /// and then executes the user-defined callback.
        /// </summary>
        public async Task ConsumerAsync()
        {
            CancellationToken cancellationToken = this.cancellationTokenSource.Token;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Wait for an item to be enqueued or Signal to be called.
                    Task task = await Task.WhenAny(this.enqueuedSignal.WaitAsync(cancellationToken), this.manualSignal.WaitAsync(cancellationToken)).ConfigureAwait(false);

                    if (this.manualSignal.IsSet)
                    {
                        // First execute the callback, then reset the event to ensure the signals sent meanwhile are ignored.
                        await this.onSignalAsync(cancellationToken).ConfigureAwait(false);
                        this.manualSignal.Reset();
                    }

                    if (this.enqueuedSignal.IsSet)
                    {
                        // First reset the event, then consume the queue to make sure all items are consumed from the queue.
                        this.enqueuedSignal.Reset();

                        T item;
                        while (this.items.TryDequeue(out item) && !cancellationToken.IsCancellationRequested)
                        {
                            await this.onEnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Add a new item to the queue and signal to the consumer task.
        /// </summary>
        /// <param name="item">Item to be added to the queue.</param>
        public void Enqueue(T item)
        {
            this.items.Enqueue(item);
            this.enqueuedSignal.Set();
        }

        /// <summary>
        /// Signals the event to ensure the on-signal callback is executed.
        /// </summary>
        public void Signal()
        {
            this.manualSignal.Set();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.cancellationTokenSource.Cancel();
            this.consumerTask.Wait();
            this.cancellationTokenSource.Dispose();
        }
    }
}