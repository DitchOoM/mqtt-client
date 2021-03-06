package com.ditchoom.mqtt.client

import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.controlpacket.QualityOfService.*
import com.ditchoom.mqtt.controlpacket.format.ReasonCode
import com.ditchoom.mqtt.topic.Filter
import com.ditchoom.mqtt.topic.Node
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ReconnectingMqttClient private constructor(
    private val scope: CoroutineScope,
    val connectionRequest: IConnectionRequest,
    val port: UShort,
    val hostname: String = "localhost",
    val useWebsockets: Boolean = false,
    val connectTimeout: Duration,
    private val persistence: Persistence = InMemoryPersistence(),
    private val keepAliveDelay: (Long) -> Duration,
    val messageSentListener: MutableSharedFlow<ControlPacket>,
) : IMqttClient {
    internal var currentClient: MqttClient? = null
    private val factory = connectionRequest.controlPacketFactory
    private val outgoingQueue = Channel<ControlPacket>()
    private val connectedFlow = MutableSharedFlow<MqttClient>()
    private val incoming = MutableSharedFlow<ControlPacket>(Channel.UNLIMITED)
    private var shouldIgnoreKeepAlive = false
    var maxReconnectionCount = ULong.MAX_VALUE
    private val pausedChannel = Channel<Unit>(1, BufferOverflow.DROP_OLDEST)
    private var pauseCount = 0
    internal var reconnectionCount = 0uL
        private set
    private val connectionStateObserversInternal =
        MutableSharedFlow<ConnectionState>(1, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    var currentConnectionState: ConnectionState = Initializing
        private set
    val connectionStateObservers: SharedFlow<ConnectionState> = connectionStateObserversInternal

    override fun toString(): String {
        return "ReconnectingClient(${connectionRequest.protocolName}:${connectionRequest.protocolVersion}) - ${connectionRequest.clientIdentifier}@$hostname:$port isConnected:${isConnected()}"
    }

    fun isConnected() = currentClient?.socketSession?.isOpen() ?: false

    val stayConnectedJob = scope.launch(CoroutineName("$this: Stay connected, reconnect if needed")) {
        updateConnectionState(Initializing)
        val initialDelay = 0.1.seconds
        val maxDelay = 30.seconds
        val factor = 2.0
        var currentDelay = initialDelay
        var isCancelled = false
        try {
            while (isActive && maxReconnectionCount >= reconnectionCount++) {
                while (pauseCount > 0) {
                    if (!pausedChannel.isEmpty) {
                        updateConnectionState(ReconnectionPaused)
                    }
                    pausedChannel.receive()
                    pauseCount--
                }
                try {
                    connect(this)
                } catch (_: CancellationException) {
                    isCancelled = true
                    return@launch
                } catch (t: Throwable) {
                    updateConnectionState(ConnectionAttemptFailed(t.message))
                    currentClient = null
                    val delayDuration = keepAliveDelay(reconnectionCount.toLong())
                    updateConnectionState(DisconnectedReconnectWithDelay(delayDuration.inWholeMilliseconds))
                    delay(delayDuration)
                    if (reconnectionCount > 1uL) {
                        currentDelay =
                            (currentDelay.inWholeMilliseconds * factor).coerceAtMost(maxDelay.toDouble(DurationUnit.MILLISECONDS)).milliseconds
                    }
                }
            }
        } finally {
            updateConnectionState(PermanentlyStopped(isCancelled || !isActive))
        }
    }

    private suspend fun connect(scope: CoroutineScope) {
        val clientConnectionDeferred =
            MqttClient.connectOnce(
                scope,
                connectionRequest,
                port,
                hostname,
                useWebsockets,
                persistence,
                connectTimeout,
                messageSentListener
            )
        updateConnectionState(AttemptingConnection)
        val clientConnection = clientConnectionDeferred.await()
        if (clientConnection is MqttClient.Companion.ClientConnection.Exception) {
            updateConnectionState(ConnectionAttemptFailed(clientConnection.throwable.message))
            throw clientConnection.throwable
        }
        val client = (clientConnection as MqttClient.Companion.ClientConnection.Connected).client
        // client should be in a connected state
        if (shouldIgnoreKeepAlive) {
            client.keepAliveJob.cancel("keep alive")
        }
        currentClient = client
        client.scope.launch(CoroutineName("$this: Outgoing packet queue")) {
            try {
                while (isActive) {
                    val outgoingPacket = outgoingQueue.receive()
                    client.sendOutgoing(outgoingPacket)
                }
            } catch (_: CancellationException) {
            }
            client.close()
        }
        client.scope.launch(CoroutineName("$this: Incoming packet queue")) {
            try {
                while (isActive) {
                    client.incoming.collect {
                        incoming.emit(it)
                    }
                }
            } catch (_: CancellationException) {
            }
            client.close()
        }
        if (connectedFlow.subscriptionCount.value > 0) {
            connectedFlow.emit(client)
        }
        updateConnectionState(Connected(reconnectionCount.toLong()))
        client.waitUntilDisconnectAsync()
        client.keepAliveJob.cancel()
    }

    private suspend fun updateConnectionState(state: ConnectionState) {
        currentConnectionState = state
        connectionStateObserversInternal.emit(state)
    }

    fun pauseReconnects() {
        pauseCount++
    }

    fun resumeReconnects() {
        if (pauseCount > 0) {
            pausedChannel.trySend(Unit)
        }
    }

    suspend fun sendDisconnect(
        reasonCode: ReasonCode = ReasonCode.NORMAL_DISCONNECTION,
        sessionExpiryIntervalSeconds: Long? = null,
        reasonString: CharSequence? = null,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList(),
    ) {
        val client = currentClient ?: return
        client.sendOutgoing(
            factory.disconnect(reasonCode, sessionExpiryIntervalSeconds, reasonString, userProperty)
        )
        client.keepAliveJob.cancel()
        client.socketSession.close()
    }

    suspend fun awaitClientConnection() = this.currentClient ?: connectedFlow.first()

    override fun publishAtMostOnce(topic: CharSequence, retain: Boolean): Deferred<Unit> {
        val nullBuffer: ParcelablePlatformBuffer? = null
        return publishAtMostOnce(topic, retain, nullBuffer)
    }

    override fun publishAtMostOnce(topic: CharSequence, retain: Boolean, payload: String?) =
        publishAtMostOnce(topic, retain, payload?.toBuffer())

    override fun publishAtMostOnce(topic: CharSequence, retain: Boolean, payload: ParcelablePlatformBuffer?) =
        scope.async {
            outgoingQueue.send(
                factory.publish(
                    qos = AT_MOST_ONCE,
                    retain = retain,
                    topicName = topic,
                    payload = payload
                )
            )
        }

    override fun publishAtLeastOnce(
        topic: CharSequence,
        retain: Boolean,
        persist: Boolean
    ): Deferred<IPublishAcknowledgment> {
        val nullBuffer: ParcelablePlatformBuffer? = null
        return publishAtLeastOnce(topic, retain, nullBuffer)
    }

    override fun publishAtLeastOnce(topic: CharSequence, retain: Boolean, payload: String?, persist: Boolean) =
        publishAtLeastOnce(topic, retain, payload?.toBuffer())

    override fun publishAtLeastOnce(
        topic: CharSequence,
        retain: Boolean,
        payload: ParcelablePlatformBuffer?,
        persist: Boolean
    ) =
        scope.async {
            val packetId = sendMessageAndAwait(persist) { packetId ->
                factory.publish(
                    qos = AT_LEAST_ONCE,
                    retain = retain,
                    topicName = topic,
                    payload = payload,
                    packetIdentifier = packetId
                )
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

    override fun publishExactlyOnce(
        topic: CharSequence,
        retain: Boolean,
        persist: Boolean
    ): DeferredPublishExactlyOnceResponse {
        val nullBuffer: ParcelablePlatformBuffer? = null
        return publishExactlyOnce(topic, retain, nullBuffer)
    }

    override fun publishExactlyOnce(topic: CharSequence, retain: Boolean, payload: String?, persist: Boolean) =
        publishExactlyOnce(topic, retain, payload?.toBuffer())


    override fun publishExactlyOnce(
        topic: CharSequence,
        retain: Boolean,
        payload: ParcelablePlatformBuffer?,
        persist: Boolean
    ): DeferredPublishExactlyOnceResponse {

        val publishReceivedDeferred = scope.async {
            val packetId = sendMessageAndAwait(persist) { packetId ->
                factory.publish(
                    qos = EXACTLY_ONCE,
                    retain = retain,
                    topicName = topic,
                    payload = payload,
                    packetIdentifier = packetId
                )
            }.first
            incoming
                .filterIsInstance<IPublishReceived>()
                .first { it.packetIdentifier == packetId }
        }
        val publishComplete = scope.async {
            val publishReceived = publishReceivedDeferred.await()
            incoming
                .filterIsInstance<IPublishComplete>()
                .first { it.packetIdentifier == publishReceived.packetIdentifier }
                .also { persistence.delete(it.packetIdentifier) }
        }
        return DeferredPublishExactlyOnceResponse(publishReceivedDeferred, publishComplete)
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
            fun ignoreKeepAlive() {
                reconnectingMqttClient.shouldIgnoreKeepAlive = true
                reconnectingMqttClient.currentClient?.keepAliveJob?.cancel("KA cancel")
            }
        }

        fun stayConnected(
            parentScope: CoroutineScope,
            persistence: Persistence,
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
            connectTimeout: Duration = 30.seconds,
            keepAliveDelay: (Long) -> Duration = { 1.seconds },
            incomingCb: ((ControlPacket) -> Unit)? = null,
            outgoingCb: ((ControlPacket) -> Unit)? = null,
        ): Pair<ReconnectingMqttClient, CancelConnection> {
            val scope = parentScope + CoroutineName("ReconnectingMqttClient $hostname:$port") + Job()
            val client = ReconnectingMqttClient(
                scope,
                connectionRequest,
                port,
                hostname,
                useWebsockets,
                connectTimeout,
                persistence,
                keepAliveDelay,
                MutableSharedFlow(),
            )
            if (incomingCb != null) {
                scope.launch {
                    client.incoming.collect {
                        incomingCb.invoke(it)
                    }
                }
            }
            if (outgoingCb != null) {
                scope.launch {
                    client.messageSentListener.collect {
                        outgoingCb.invoke(it)
                    }
                }
            }
            val cancelConnection = CancelConnection(client)
            return Pair(client, cancelConnection)
        }
    }
}