@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.ISubscription.RetainHandling
import com.ditchoom.mqtt.controlpacket.QualityOfService.*
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClient private constructor(
    private val scope: CoroutineScope,
    private val socketSession: MqttSocketSession,
    val connectionRequest: IConnectionRequest,
    private val outgoing: Channel<ControlPacket>,
    private val incoming: SharedFlow<ControlPacket>,
): SuspendCloseable {

    var pingRequestCount = 0L
        private set
    var pingResponseCount = 0L
        private set

    private val keepAliveDuration = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toInt())
    private val packetFactory = connectionRequest.controlPacketFactory
    private val lastUsedPacketIdentifier = atomic(0)
    private fun nextPacketIdentifier() =
        (lastUsedPacketIdentifier.incrementAndGet()).mod(UShort.MAX_VALUE.toInt())

    fun publishAtMostOnce(topic: CharSequence) = publishAtMostOnce(topic, null as? PlatformBuffer?)
    fun publishAtMostOnce(topic: CharSequence, payload: String? = null) =
        publishAtMostOnce(topic, payload?.toBuffer())

    fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        sendOutgoing(packetFactory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    fun publishAtLeastOnce(topic: CharSequence) = publishAtLeastOnce(topic, null as? PlatformBuffer?)
    fun publishAtLeastOnce(topic: CharSequence, payload: String? = null) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        sendOutgoing(
            packetFactory.publish(
                qos = AT_LEAST_ONCE,
                topicName = topic,
                payload = payload,
                packetIdentifier = packetIdentifier
            )
        )
        return@async incoming
            .filterIsInstance<IPublishAcknowledgment>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
    }

    fun publishExactlyOnce(topic: CharSequence) = publishExactlyOnce(topic, null as? PlatformBuffer?)
    fun publishExactlyOnce(topic: CharSequence, payload: String? = null) =
        publishExactlyOnce(topic, payload?.toBuffer())

    fun publishExactlyOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        sendOutgoing(
            packetFactory.publish(
                qos = EXACTLY_ONCE, topicName = topic, payload = payload, packetIdentifier = packetIdentifier))
        val publishReceived = incoming
            .filterIsInstance<IPublishReceived>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
        sendOutgoing(publishReceived.expectedResponse())
        incoming
            .filterIsInstance<IPublishComplete>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
    }

    fun subscribe(
        topicFilter: CharSequence,
        maximumQos: QualityOfService = AT_LEAST_ONCE,
        noLocal: Boolean = false,
        retainAsPublished: Boolean = false,
        retainHandling: RetainHandling = RetainHandling.SEND_RETAINED_MESSAGES_AT_TIME_OF_SUBSCRIBE,
        serverReference: CharSequence? = null,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList(),
        callback: ((IPublishMessage) -> Unit)? = null
    ) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val sub = packetFactory.subscribe(
            packetIdentifier,
            topicFilter,
            maximumQos,
            noLocal,
            retainAsPublished,
            retainHandling,
            serverReference,
            userProperty
        )
        sendOutgoing(sub)
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
        if (callback != null) {
            observe(Filter(topicFilter), callback)
        }
        return@async Pair(sub, subscribeAcknowledgment)
    }

    fun subscribe(
        subscriptions: Set<ISubscription>,
        serverReference: CharSequence? = null,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList(),
        callback: ((IPublishMessage) -> Unit)? = null
    ) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val sub = packetFactory.subscribe(packetIdentifier, subscriptions, serverReference, userProperty)
        sendOutgoing(sub)
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()

        if (callback != null) {
            observeMany(subscriptions, callback)
        }
        return@async Pair(sub, subscribeAcknowledgment)
    }

    fun unsubscribe(
        topic: String,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ) = unsubscribe(setOf(topic), userProperty)

    fun unsubscribe(
        topics: Set<String>,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val unsub = packetFactory.unsubscribe(packetIdentifier, topics, userProperty)
        sendOutgoing(unsub)
        val unsuback = incoming
            .filterIsInstance<IUnsubscribeAcknowledgment>()
            .first()
        Pair(unsub, unsuback)
    }

    fun observeMany(subscriptions: Set<ISubscription>, callback: (IPublishMessage) -> Unit) {
        val topicFilters = subscriptions.map { checkNotNull(it.topicFilter.validate()) }
        scope.launch {
            incoming
                .filterIsInstance<IPublishMessage>()
                .filter { pub ->
                    topicFilters.firstOrNull { filter ->
                        filter.matchesTopic(Node.parse(pub.topic))
                    } != null
                }
                .collect {
                    callback(it)
                }
        }
    }

    fun observe(topicFilter: Filter, callback: (IPublishMessage) -> Unit) {
        val topicNode = checkNotNull(topicFilter.validate()) { "Failed to validate topic filter" }
        scope.launch {
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


    fun ping() = scope.async {
        sendOutgoing(packetFactory.pingRequest())
        pingRequestCount++
        incoming
            .filterIsInstance<IPingResponse>()
            .first()
            .also {
                pingResponseCount++
            }
    }

    fun observePongs(callback: (IPingResponse) -> Unit) = scope.launch {
        incoming.filterIsInstance<IPingResponse>()
            .collect {
                callback(it)
            }
    }

    private suspend fun sendOutgoing(packet: ControlPacket) {
        outgoing.send(packet)
        // ensure the outgoing message has sent before un-suspending
        yield()
    }


    private fun startKeepAliveTimer() = scope.launch {
        if (keepAliveDuration < Duration.Companion.seconds(1)) {
            return@launch
        }
        while (isActive && socketSession.isOpen()) {
            var currentDelay = keepAliveDuration.minus(socketSession.lastMessageReceivedTimestamp.elapsedNow())
            while (currentDelay > Duration.Companion.seconds(0)) {
                delay(currentDelay)
                currentDelay = keepAliveDuration.minus(socketSession.lastMessageReceivedTimestamp.elapsedNow())
            }
            ping().await()
            delay(keepAliveDuration)
        }
    }

    override suspend fun close() {
        socketSession.write(packetFactory.disconnect())
        socketSession.close()
        scope.cancel()
    }

    @ExperimentalTime
    companion object {
        fun connect(
            scope: CoroutineScope,
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
        ) = scope.async {
            val clientScope = this + Job()
            val outgoing = Channel<ControlPacket>()
            val incoming = MutableSharedFlow<ControlPacket>()
            val socketSession = MqttSocketSession.openConnection(connectionRequest, port, hostname, useWebsockets)
            val client = MqttClient(clientScope, socketSession,  connectionRequest, outgoing, incoming)
            clientScope.launch {
                try {
                    while (socketSession.isOpen()) {
                        val read = socketSession.read()
                        incoming.emit(read)
                        if (read is IDisconnectNotification) {
                            client.close()
                        }
                    }
                } catch (e: Exception) {}
            }
            clientScope.launch {
                while (socketSession.isOpen()) {
                    val payload = outgoing.receive()
                    socketSession.write(payload)
                }
            }
            client.startKeepAliveTimer()
            client
        }
    }
}