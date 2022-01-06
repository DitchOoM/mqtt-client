package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.QualityOfService.AT_MOST_ONCE
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
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
): IMqttClient {
    internal var currentClient :MqttClient? = null
    private val persistence = InMemoryPersistence()
    private val factory = connectionRequest.controlPacketFactory
    private val outgoingQueue = Channel<ControlPacket>()
    private val connectedFlow = MutableSharedFlow<MqttClient>()
    private val incoming = MutableSharedFlow<ControlPacket>(Channel.UNLIMITED)
    private var shouldIgnoreKeepAlive = false
    var maxReconnectionCount = ULong.MAX_VALUE
    internal var reconnectionCount = 0uL
        private set

    init {
        scope.launch(CoroutineName("$this: Automatic Message Handler")) {
            incoming.collect { packet ->
                when (packet) {
                    is IPublishAcknowledgment -> persistence.delete(packet.packetIdentifier)
                    is IPublishReceived -> publishExactlyOnceInternalStep2(packet)
                    is IPublishComplete -> persistence.delete(packet.packetIdentifier)
                    is ISubscribeAcknowledgement -> persistence.delete(packet.packetIdentifier)
                    is IUnsubscribeAcknowledgment -> persistence.delete(packet.packetIdentifier)
                }
            }
        }
    }

    fun isConnected() = currentClient?.socketSession?.isOpen() ?: false

    val stayConnectedJob = scope.launch(CoroutineName("$this: Stay connected, reconnect if needed")) {
        while (isActive && maxReconnectionCount >= reconnectionCount) {
            println("reconnect loop $reconnectionCount out of $maxReconnectionCount")
            try {
                val client = MqttClient.connectOnce(this, connectionRequest, port, hostname, useWebsockets).await()
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

    suspend fun awaitClientConnection() = connectedFlow.asSharedFlow().first()

    override fun publishAtMostOnce(topic: CharSequence) :Deferred<Unit> {
        val nullBuffer :PlatformBuffer? = null
        return publishAtMostOnce(topic, nullBuffer)
    }

    override fun publishAtMostOnce(topic: CharSequence, payload: String?)
        = publishAtMostOnce(topic, payload?.toBuffer())

    override fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        outgoingQueue.send(factory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    override fun publishAtLeastOnce(topic: CharSequence) :Deferred<IPublishAcknowledgment> {
        val nullBuffer :PlatformBuffer? = null
        return publishAtLeastOnce(topic, nullBuffer)
    }

    override fun publishAtLeastOnce(topic: CharSequence, payload: String?) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    override fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        val packetId = persistence.nextPacketIdentifier()
        val pub = factory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload, packetIdentifier = packetId)
        persistence.save(packetId, pub)
        outgoingQueue.send(pub)
        incoming
            .filterIsInstance<IPublishAcknowledgment>()
            .filter { it.packetIdentifier == packetId }
            .first()
    }

    override fun publishExactlyOnce(topic: CharSequence): Deferred<Unit> {
        val nullBuffer :PlatformBuffer? = null
        return publishExactlyOnce(topic, nullBuffer)
    }

    override fun publishExactlyOnce(topic: CharSequence, payload: String?) =
        publishExactlyOnce(topic, payload?.toBuffer())

    override fun publishExactlyOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        val packetIdentifier = persistence.nextPacketIdentifier()
        val packet = factory.publish(
            qos = QualityOfService.EXACTLY_ONCE, topicName = topic, payload = payload, packetIdentifier = packetIdentifier)
        persistence.save(packetIdentifier, packet)
        outgoingQueue.send(packet)
        val publishReceived = incoming
            .filterIsInstance<IPublishReceived>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
        publishExactlyOnceInternalStep2(publishReceived)
    }
    private suspend fun publishExactlyOnceInternalStep2(publishReceived: IPublishReceived) {
        val response = publishReceived.expectedResponse()
        persistence.save(response.packetIdentifier, response)
        outgoingQueue.send(response)
        incoming
            .filterIsInstance<IPublishComplete>()
            .filter { it.packetIdentifier == publishReceived.packetIdentifier }
            .first()
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
        callback: ((IPublishMessage) -> Unit)?
    )= scope.async {
        val packetIdentifier = persistence.nextPacketIdentifier()
        val sub = factory.subscribe(
            packetIdentifier,
            topicFilter,
            maximumQos,
            noLocal,
            retainAsPublished,
            retainHandling,
            serverReference,
            userProperty
        )
        persistence.save(packetIdentifier, sub)
        outgoingQueue.send(sub)
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
        if (callback != null) {
            observe(Filter(topicFilter), callback)
        }
        return@async Pair(sub, subscribeAcknowledgment)
    }

    override fun unsubscribe(
        topic: String,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = unsubscribe(setOf(topic), userProperty)

    override fun unsubscribe(
        topics: Set<String>,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = scope.async {
        val packetIdentifier = persistence.nextPacketIdentifier()
        val unsub = factory.unsubscribe(packetIdentifier, topics, userProperty)
        persistence.save(packetIdentifier, unsub)
        outgoingQueue.send(unsub)
        val unsuback = incoming
            .filterIsInstance<IUnsubscribeAcknowledgment>()
            .first()
        Pair(unsub, unsuback)
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
            val scope = parentScope + Job()
            val client = ReconnectingMqttClient(scope, connectionRequest, port, hostname, useWebsockets)
            val cancelConnection = CancelConnection(client)
            return Pair(client, cancelConnection)
        }
    }
}