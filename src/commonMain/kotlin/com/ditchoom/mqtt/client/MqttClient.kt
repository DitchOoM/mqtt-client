@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.client.net.MqttSocketSession
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.ISubscription.RetainHandling
import com.ditchoom.mqtt.controlpacket.QualityOfService.AT_LEAST_ONCE
import com.ditchoom.mqtt.controlpacket.QualityOfService.AT_MOST_ONCE
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClient private constructor(
    private val scope: CoroutineScope,
    private val socketSession: MqttSocketSession,
    connectionRequest: IConnectionRequest,
    private val outgoing:Channel<ControlPacket>,
    private val incoming: SharedFlow<ControlPacket>,
): SuspendCloseable {
    private val packetFactory = connectionRequest.controlPacketFactory
    private val lastUsedPacketIdentifier = atomic(0)
    private fun nextPacketIdentifier() = (lastUsedPacketIdentifier.incrementAndGet()).mod(UShort.SIZE_BYTES)

    fun publishAtMostOnce(topic: CharSequence, payload: String? = null) =
        publishAtMostOnce(topic, payload?.toBuffer())

    fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        outgoing.send(packetFactory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    fun publishAtLeastOnce(topic: CharSequence, payload: String? = null) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        outgoing.send(
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
            .take(1)
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
        outgoing.send(sub)
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .filter { it.packetIdentifier == packetIdentifier }
            .take(1)
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
        outgoing.send(sub)
        val subscribeAcknowledgment = incoming
            .filterIsInstance<ISubscribeAcknowledgement>()
            .filter { it.packetIdentifier == packetIdentifier }
            .take(1)
            .first()

        if (callback != null) {
            observeMany(subscriptions, callback)
        }
        return@async Pair(sub, subscribeAcknowledgment)
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

    override suspend fun close() {
        outgoing.send(packetFactory.disconnect())
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
        ) = scope.async {
            val clientScope = this + Job()
            val outgoing = Channel<ControlPacket>()
            val incoming = MutableSharedFlow<ControlPacket>()
            val socketSession = MqttSocketSession.openConnection(clientScope, connectionRequest, port, hostname)
            val client = MqttClient(clientScope, socketSession,  connectionRequest, outgoing, incoming)
            clientScope.launch {
                while (socketSession.isOpen()) {
                    val read = socketSession.read()
                    incoming.emit(read)
                    if (read is IDisconnectNotification) {
                        client.close()
                    }
                }
            }
            clientScope.launch {
                while (socketSession.isOpen()) {
                    val payload = outgoing.receive()
                    socketSession.write(payload)
                }
            }
            client
        }
    }
}