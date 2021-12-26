@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.ISubscription.RetainHandling
import com.ditchoom.mqtt.controlpacket.QualityOfService.*
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClient private constructor(
    internal val scope: CoroutineScope,
    internal val socketSession: MqttSocketSession,
    val connectionRequest: IConnectionRequest,
    private val outgoing: Channel<ControlPacket>,
    internal val incoming: SharedFlow<ControlPacket>,
//    private val persistence: Persistence,
): IMqttClient {
    var pingRequestCount = 0L
        private set
    var pingResponseCount = 0L
        private set

    private val keepAliveDuration = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toInt())
    private val packetFactory = connectionRequest.controlPacketFactory
    private var count = 1

    internal val keepAliveJob = startKeepAliveTimer()

    init {
//        scope.launch {
//            incoming.collect { packet ->
//                when (packet) {
//                    is IPublishAcknowledgment -> persistence.delete(packet.packetIdentifier)
//                    is IPublishReceived -> publishExactlyOnceInternalStep2(packet)
//                    is IPublishComplete -> persistence.delete(packet.packetIdentifier)
//                    is ISubscribeAcknowledgement -> persistence.delete(packet.packetIdentifier)
//                    is IUnsubscribeAcknowledgment -> persistence.delete(packet.packetIdentifier)
//                }
//            }
//        }
    }


    private suspend fun nextPacketIdentifier() = count++

    override fun publishAtMostOnce(topic: CharSequence) = publishAtMostOnce(topic, null as? PlatformBuffer?)
    override fun publishAtMostOnce(topic: CharSequence, payload: String?) =
        publishAtMostOnce(topic, payload?.toBuffer())

    override fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        sendOutgoing(packetFactory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    override fun publishAtLeastOnce(topic: CharSequence) = publishAtLeastOnce(topic, null as? PlatformBuffer?)
    override fun publishAtLeastOnce(topic: CharSequence, payload: String?) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    override fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val packet = packetFactory.publish(
            qos = AT_LEAST_ONCE,
            topicName = topic,
            payload = payload,
            packetIdentifier = packetIdentifier
        )
        //persistence.save(packetIdentifier, packet)
        sendOutgoing(packet)
        return@async incoming
            .filterIsInstance<IPublishAcknowledgment>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
    }

    override fun publishExactlyOnce(topic: CharSequence) = publishExactlyOnce(topic, null as? PlatformBuffer?)
    override fun publishExactlyOnce(topic: CharSequence, payload: String?) =
        publishExactlyOnce(topic, payload?.toBuffer())

    override fun publishExactlyOnce(topic: CharSequence, payload: PlatformBuffer?) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val packet = packetFactory.publish(
            qos = EXACTLY_ONCE, topicName = topic, payload = payload, packetIdentifier = packetIdentifier)
        //persistence.save(packetIdentifier, packet)
        sendOutgoing(packet)
        val publishReceived = incoming
            .filterIsInstance<IPublishReceived>()
            .filter { it.packetIdentifier == packetIdentifier }
            .first()
        publishExactlyOnceInternalStep2(publishReceived)
    }
    private suspend fun publishExactlyOnceInternalStep2(publishReceived: IPublishReceived) {
        val response = publishReceived.expectedResponse()
        //persistence.save(response.packetIdentifier, response)
        sendOutgoing(response)
        incoming
            .filterIsInstance<IPublishComplete>()
            .filter { it.packetIdentifier == publishReceived.packetIdentifier }
            .first()
            //.also { persistence.delete(it.packetIdentifier) }
    }

    override fun subscribe(
        topicFilter: CharSequence,
        maximumQos: QualityOfService,
        noLocal: Boolean,
        retainAsPublished: Boolean,
        retainHandling: RetainHandling,
        serverReference: CharSequence?,
        userProperty: List<Pair<CharSequence, CharSequence>>,
        callback: ((IPublishMessage) -> Unit)?
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
        //persistence.save(packetIdentifier, sub)
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

    override fun unsubscribe(
        topic: String,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = unsubscribe(setOf(topic), userProperty)

    override fun unsubscribe(
        topics: Set<String>,
        userProperty: List<Pair<CharSequence, CharSequence>>
    ) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        val unsub = packetFactory.unsubscribe(packetIdentifier, topics, userProperty)
        //persistence.save(packetIdentifier, unsub)
        sendOutgoing(unsub)
        val unsuback = incoming
            .filterIsInstance<IUnsubscribeAcknowledgment>()
            .first()
        Pair(unsub, unsuback)
    }

    override fun observe(topicFilter: Filter, callback: (IPublishMessage) -> Unit) {
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


    override fun ping() = scope.async {
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

    internal suspend fun sendOutgoing(packet: ControlPacket) {
        outgoing.send(packet)
    }

    private fun startKeepAliveTimer() = scope.launch {
        try {
            if (keepAliveDuration < Duration.Companion.seconds(1)) {
                return@launch
            }
            while (isActive && socketSession.isOpen()) {
                var currentDelay = keepAliveDuration.minus(socketSession.lastMessageReceivedTimestamp.elapsedNow())
                while (currentDelay > Duration.Companion.seconds(0) && isActive) {
                    delay(currentDelay)
                    currentDelay = keepAliveDuration.minus(socketSession.lastMessageReceivedTimestamp.elapsedNow())
                }
                ping()
                delay(keepAliveDuration)
            }
        } catch (e: CancellationException) {
        }
    }

    override suspend fun close() {
        try {
            socketSession.write(packetFactory.disconnect())
        } catch (e: NullPointerException) {}
        socketSession.close()
    }

    @ExperimentalTime
    companion object {

        fun connectOnce(
            scope: CoroutineScope,
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
        ): Deferred<MqttClient> = scope.async {
//            val persistence: Persistence = InMemoryPersistence()
            val clientScope = scope + Job()
            val outgoing = Channel<ControlPacket>()
            val incoming = MutableSharedFlow<ControlPacket>()
            val socketSession = MqttSocketSession.openConnection(connectionRequest, port, hostname, useWebsockets)
            val client = MqttClient(clientScope, socketSession,  connectionRequest, outgoing, incoming)//, persistence)
            clientScope.launch {
                try {
                    while (socketSession.isOpen()) {
                        val read = socketSession.read()
                        incoming.emit(read)
                        if (read is IDisconnectNotification) {
                            client.close()
                        }
                    }
                } catch (t: Throwable) {
                }
                client.close()
            }
            clientScope.launch {
                try {
                    // First dequeue all the queued packets that were not acknowledged
//                var queuedPacket = persistence.readNextControlPacketOrNull()
//                while (socketSession.isOpen() && queuedPacket != null) {
//                    socketSession.write(queuedPacket)
//                    queuedPacket = persistence.readNextControlPacketOrNull()
//                }
                    // Now write the other messages
                    while (socketSession.isOpen()) {
                        val payload = outgoing.receive()
                        socketSession.write(payload)
                    }
                    client.close()
                } catch (e: NullPointerException) {
                    // ignore
                }
            }
            client
        }
    }
}