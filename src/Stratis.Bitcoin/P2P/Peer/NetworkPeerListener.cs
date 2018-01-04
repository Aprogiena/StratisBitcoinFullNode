using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Stratis.Bitcoin.P2P.Protocol;
using Stratis.Bitcoin.P2P.Protocol.Payloads;
using Stratis.Bitcoin.Utilities;

namespace Stratis.Bitcoin.P2P.Peer
{
    /// <summary>
    /// Implements a queue of incoming messages with async event that signals when the queue is not empty.
    /// </summary>
    /// <remarks>
    /// By itself, this queue is not thread-safe.
    /// </remarks>
    public class UnprocessedMessageQueue
    {
        /// <summary>Internal storage of the messages in the queue.</summary>
        private readonly Queue<IncomingMessage> queue;

        /// <summary>Async event that signals when the queue is not empty.</summary>
        private readonly AsyncManualResetEvent signal;

        /// <summary>Number of messages in the queue.</summary>
        public int Count
        {
            get
            {
                return this.queue.Count;
            }
        }

        /// <summary>
        /// Initializes an instance of the object.
        /// </summary>
        public UnprocessedMessageQueue()
        {
            this.queue = new Queue<IncomingMessage>();
            this.signal = new AsyncManualResetEvent();
        }

        /// <summary>
        /// Adds a new message to the queue and signals the event.
        /// </summary>
        /// <param name="message">Message to add.</param>
        public void Enqueue(IncomingMessage message)
        {
            this.queue.Enqueue(message);
            this.signal.Set();
        }

        /// <summary>
        /// Removes a message from a queue and resets the event if the queue becomes empty.
        /// </summary>
        /// <returns>Message removed from the queue.</returns>
        public IncomingMessage Dequeue()
        {
            IncomingMessage message = this.queue.Dequeue();

            if (this.queue.Count == 0)
                this.signal.Set();

            return message;
        }

        /// <summary>
        /// Waits until the event signals, which means there is a message in the queue. 
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to abort the wait.</param>
        public async Task WaitAsync(CancellationToken cancellationToken)
        {
            await this.signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Implements a consumer of messages of specific payload type received from a peer.
    /// </summary>
    public class NetworkPeerListener : IMessageListener<IncomingMessage>, IDisposable
    {
        /// <summary>Lock object to protect access to <see cref="unprocessedMessages"/> and objects stored in it.</summary>
        private readonly object lockObject;

        /// <remarks>All access to this object or the objects stored in this dictionary has to be protected by <see cref="lockObject"/>.</remarks>
        private readonly Dictionary<Type, UnprocessedMessageQueue> unprocessedMessages;

        /// <summary>Connected network peer that sends the messages that the listener consumes.</summary>
        private readonly NetworkPeer peer;
        
        /// <summary>Subscription to the message producer of the network peer.</summary>
        private readonly IDisposable subscription;

        /// <summary>
        /// Initializes an instance of the object and subscribes for the messages to the peer's network producer.
        /// </summary>
        /// <param name="peer">Connected network peer that sends the messages that the listener consumes.</param>
        public NetworkPeerListener(NetworkPeer peer)
        {
            this.lockObject = new object();
            this.unprocessedMessages = new Dictionary<Type, UnprocessedMessageQueue>();

            this.peer = peer;
            this.subscription = peer.MessageProducer.AddMessageListener(this);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Adds the newly received message to the queue of messages with same payload type.
        /// Then sets the event to signal a new message is available.
        /// </remarks>
        public void PushMessage(IncomingMessage message)
        {
            Type payloadType = message.Message.Payload.GetType();
            lock (this.lockObject)
            {
                UnprocessedMessageQueue messageQueue = this.GetPayloadQueueLocked(payloadType);
                messageQueue.Enqueue(message);
            }
        }

        /// <summary>
        /// Retrieves a queue of unprocessed message of a specific payload type.
        /// If such a queue does not exist yet, it is created.
        /// </summary>
        /// <param name="payloadType">Type of the payload for which to obtain the queue.</param>
        /// <returns>Queue of unprocessed messages of the given type.</returns>
        /// <remarks>The caller of this method is responsible for holding <see cref="lockObject"/>.</remarks>
        private UnprocessedMessageQueue GetPayloadQueueLocked(Type payloadType)
        {
            UnprocessedMessageQueue messageQueue;
            
            if (!this.unprocessedMessages.TryGetValue(payloadType.GetType(), out messageQueue))
            {
                messageQueue = new UnprocessedMessageQueue();
                this.unprocessedMessages.Add(payloadType.GetType(), messageQueue);
            }

            return messageQueue;
        }

        /// <summary>
        /// Waits until a message of a specific payload type is received from the peer.
        /// </summary>
        /// <typeparam name="TPayload">Type of the payload to wait for.</typeparam>
        /// <param name="cancellationToken">Cancellation token to abort the waiting operation.</param>
        /// <returns>Message of the specific payload received from the peer.</returns>
        private async Task<IncomingMessage> ReceiveMessageAsync<TPayload>(CancellationToken cancellationToken) where TPayload : Payload
        {
            UnprocessedMessageQueue messageQueue;

            // First we check if the requested message is not already available.
            lock (this.lockObject)
            {
                messageQueue = this.GetPayloadQueueLocked(typeof(TPayload));
                if (messageQueue.Count > 0)
                    return messageQueue.Dequeue();
            }

            // If not, we wait until we receive it or cancellation is triggered.
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await messageQueue.WaitAsync(cancellationToken).ConfigureAwait(false);

                // Note that another thread could consume the message before us, 
                // so dequeue safely and loop if nothing is available.
                lock (this.lockObject)
                {
                    if (messageQueue.Count > 0)
                        return messageQueue.Dequeue();
                }
            }
        }

        /// <summary>
        /// Waits until a payload of a specific type is received from the peer.
        /// </summary>
        /// <typeparam name="TPayload">Type of the payload to wait for.</typeparam>
        /// <param name="cancellationToken">Cancellation token to abort the waiting operation.</param>
        /// <returns>Payload of a specific type received from the peer.</returns>
        public async Task<TPayload> ReceivePayloadAsync<TPayload>(CancellationToken cancellationToken = default(CancellationToken)) where TPayload : Payload
        {
            if (!this.peer.IsConnected)
                throw new InvalidOperationException("The peer is not in a connected state");

            TPayload result = null;

            CancellationTokenSource cancellationSource = null;
            if (cancellationToken != default(CancellationToken))
            {
                cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.peer.Connection.CancellationSource.Token);
                cancellationToken = cancellationSource.Token;
            }
            else cancellationToken = this.peer.Connection.CancellationSource.Token;

            try
            {
                IncomingMessage message = await this.ReceiveMessageAsync<TPayload>(cancellationToken).ConfigureAwait(false);
                result = (TPayload)message.Message.Payload;
            }
            finally
            {
                cancellationSource?.Dispose();
            }

            return result;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.subscription.Dispose();
        }
    }
}