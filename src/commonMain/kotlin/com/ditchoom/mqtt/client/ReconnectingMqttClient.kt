package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.QualityOfService.*
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ReconnectingMqttClient private constructor(
    private val scope: CoroutineScope,
    private val connectionRequest: IConnectionRequest,
    private val port: UShort,
    private val hostname: String = "localhost",
    private val useWebsockets: Boolean = false,
    private val persistence: Persistence = InMemoryPersistence(),
) : IMqttClient {
    internal var currentClient: MqttClient? = null
    private val factory = connectionRequest.controlPacketFactory
    private val outgoingQueue = Channel<ControlPacket>()
    private val connectedFlow = MutableSharedFlow<MqttClient>()
    private val incoming = MutableSharedFlow<ControlPacket>(Channel.UNLIMITED)
    private var shouldIgnoreKeepAlive = false
    var maxReconnectionCount = ULong.MAX_VALUE
    private val pausedChannel = Channel<Unit>(1, BufferOverflow.DROP_OLDEST)
    private var isPaused = false
    internal var reconnectionCount = 0uL
        private set

    fun isConnected() = currentClient?.socketSession?.isOpen() ?: false

    val stayConnectedJob = scope.launch(CoroutineName("$this: Stay connected, reconnect if needed")) {
        while (isActive && maxReconnectionCount >= reconnectionCount) {
            if (isPaused) {
                pausedChannel.receive()
                isPaused = false
            }
            try {
                val client =
                    MqttClient.connectOnce(this, connectionRequest, port, hostname, useWebsockets, persistence).await()
                // client should be in a connected state
                if (shouldIgnoreKeepAlive) {
                    client.keepAliveJob.cancel("keep alive")
                }
                currentClient = client
                client.scope.launch(CoroutineName("$this: Outgoing packet queue")) {
                    try {
                        while (isActive && client.socketSession.isOpen()) {
                            val outgoingPacket = outgoingQueue.receive()
                            client.sendOutgoing(outgoingPacket)
                        }
                    } catch (e: CancellationException) {
                    }
                    client.close()
                }
                client.scope.launch(CoroutineName("$this: Incoming packet queue")) {
                    try {
                        while (isActive && client.socketSession.isOpen()) {
                            client.incoming.collect {
                                incoming.emit(it)
                            }
                        }
                    } catch (e: CancellationException) {
                    }
                    client.close()
                }
                if (connectedFlow.subscriptionCount.value > 0) {
                    connectedFlow.emit(client)
                }
                client.waitUntilDisconnectAsync()
            } catch (e: CancellationException) {
                e.printStackTrace()

            } catch (t: Throwable) {
                t.printStackTrace()
            } finally {
                currentClient = null
                reconnectionCount++
            }
        }
    }

    fun pauseReconnects() {
        isPaused = true
    }

    fun resumeReconnects() {
        if (isPaused) {
            pausedChannel.trySend(Unit)
        }
    }

    suspend fun awaitClientConnection() = connectedFlow.asSharedFlow().first()

    override fun publishAtMostOnce(topic: CharSequence): Deferred<Unit> {
        val nullBuffer: PlatformBuffer? = null
        return publishAtMostOnce(topic, nullBuffer)
    }

    override fun publishAtMostOnce(topic: CharSequence, payload: String?) =
        publishAtMostOnce(topic, payload?.toBuffer())

    override fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        outgoingQueue.send(factory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    override fun publishAtLeastOnce(topic: CharSequence, persist: Boolean): Deferred<IPublishAcknowledgment> {
        val nullBuffer: PlatformBuffer? = null
        return publishAtLeastOnce(topic, nullBuffer)
    }

    override fun publishAtLeastOnce(topic: CharSequence, payload: String?, persist: Boolean) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    override fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer?, persist: Boolean) = scope.async {
        val packetId = sendMessageAndAwait(persist) { packetId ->
            factory.publish(qos = AT_LEAST_ONCE, topicName = topic, payload = payload, packetIdentifier = packetId)
        }.first
        incoming
            .filterIsInstance<IPublishAcknowledgment>()
            .first { it.packetIdentifier == packetId }
    }

    private suspend fun sendMessageAndAwait(
        persist: Boolean,
        cb: suspend (Int) -> ControlPacket
    ): Pair<Int, ControlPacket> {
        val packetId = persistence.nextPacketIdentifier(persist)
        val packet = cb(packetId)
        if (persist) {
            persistence.save(packetId, packet)
        }
        outgoingQueue.send(packet)
        return Pair(packetId, packet)
    }

    override fun publishExactlyOnce(topic: CharSequence, persist: Boolean): Deferred<Unit> {
        val nullBuffer: PlatformBuffer? = null
        return publishExactlyOnce(topic, nullBuffer)
    }

    override fun publishExactlyOnce(topic: CharSequence, payload: String?, persist: Boolean) =
        publishExactlyOnce(topic, payload?.toBuffer())

    override fun publishExactlyOnce(topic: CharSequence, payload: PlatformBuffer?, persist: Boolean) = scope.async {
        val packetId = sendMessageAndAwait(persist) { packetId ->
            factory.publish(
                qos = EXACTLY_ONCE,
                topicName = topic,
                payload = payload,
                packetIdentifier = packetId
            )
        }.first
        val publishReceived = incoming
            .filterIsInstance<IPublishReceived>()
            .first { it.packetIdentifier == packetId }
        publishExactlyOnceInternalStep2(publishReceived)
    }

    private suspend fun publishExactlyOnceInternalStep2(publishReceived: IPublishReceived) {
        val response = publishReceived.expectedResponse()
        persistence.save(response.packetIdentifier, response)
        outgoingQueue.send(response)
        incoming
            .filterIsInstance<IPublishComplete>()
            .first { it.packetIdentifier == publishReceived.packetIdentifier }
            .also { persistence.delete(it.packetIdentifier) }
    }

    override fun subscribe(
        topicFilter: CharSequence,
        maximumQos: QualityOfService,
        noLocal: Boolean,
        retainAsPublished: Boolean,
        retainHandling: ISubscription.RetainHandling,
        serverReference: CharSequence?,
        userProperty: List<Pair<CharSequence, CharSequence>>,
        persist: Boolean,
        callback: ((IPublishMessage) -> Unit)?
    ) = scope.async {
        val (packetIdentifier, sub) = sendMessageAndAwait(persist) { packetId ->
            factory.subscribe(
                packetId,
                topicFilter,
                maximumQos,
                noLocal,
                retainAsPublished,
                retainHandling,
                serverReference,
                userProperty
            )
        }
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .first { it.packetIdentifier == packetIdentifier }
        if (callback != null) {
            observe(Filter(topicFilter), callback)
        }
        return@async Pair(sub as ISubscribeRequest, subscribeAcknowledgment)
    }

    override fun unsubscribe(
        topic: String,
        persist: Boolean,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = unsubscribe(setOf(topic), userProperty = userProperty)

    override fun unsubscribe(
        topics: Set<String>,
        persist: Boolean,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = scope.async {
        val (packetIdentifier, unsub) = sendMessageAndAwait(persist) { packetId ->
            factory.unsubscribe(packetId, topics, userProperty)
        }
        val unsuback = incoming
            .filterIsInstance<IUnsubscribeAcknowledgment>()
            .first { it.packetIdentifier == packetIdentifier }
        Pair(unsub as IUnsubscribeRequest, unsuback)
    }

    override fun observe(topicFilter: Filter, callback: (IPublishMessage) -> Unit) {
        val topicNode = checkNotNull(topicFilter.validate()) { "Failed to validate topic filter" }
        scope.launch(CoroutineName("$this: Filtering for $topicFilter")) {
            incoming
                .filterIsInstance<IPublishMessage>()
                .filter {
                    topicNode.matchesTopic(Node.parse(it.topic))
                }
                .collect {
                    callback(it)
                }
        }
    }

    override fun ping() = scope.async {
        outgoingQueue.send(factory.pingRequest())
        incoming
            .filterIsInstance<IPingResponse>()
            .first()
    }

    override suspend fun close() {
        scope.cancel()
    }

    companion object {
        class CancelConnection(private val reconnectingMqttClient: ReconnectingMqttClient) {
            internal fun ignoreKeepAlive() {
                reconnectingMqttClient.shouldIgnoreKeepAlive = true
                reconnectingMqttClient.currentClient?.keepAliveJob?.cancel("KA cancel")
            }
        }

        fun stayConnected(
            parentScope: CoroutineScope,
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
        ): Pair<IMqttClient, CancelConnection> {
            val scope = parentScope + CoroutineName("ReconnectingMqttClient $hostname:$port") + Job()
            val client = ReconnectingMqttClient(scope, connectionRequest, port, hostname, useWebsockets)
            val cancelConnection = CancelConnection(client)
            return Pair(client, cancelConnection)
        }
    }
}