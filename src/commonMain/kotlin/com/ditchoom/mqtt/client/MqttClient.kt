@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.buffer.toBuffer
import com.ditchoom.mqtt.client.net.MqttSocketSession
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.mqtt.controlpacket.IPublishAcknowledgment
import com.ditchoom.mqtt.controlpacket.QualityOfService.AT_LEAST_ONCE
import com.ditchoom.mqtt.controlpacket.QualityOfService.AT_MOST_ONCE
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
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

    fun currentConnack() = socketSession.connack

    private fun nextPacketIdentifier() = (lastUsedPacketIdentifier.incrementAndGet()).mod(UShort.SIZE_BYTES)

    fun send(packet: ControlPacket) = scope.async {  outgoing.send(packet) }
    fun publishAtMostOnce(topic: CharSequence, payload: String? = null) =
        publishAtMostOnce(topic, payload?.toBuffer())

    fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        outgoing.send(packetFactory.publish(qos = AT_MOST_ONCE, topicName = topic, payload = payload))
    }

    fun publishAtLeastOnce(topic: CharSequence, payload: String? = null) =
        publishAtLeastOnce(topic, payload?.toBuffer())

    fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer? = null) = scope.async {
        val packetIdentifier = nextPacketIdentifier()
        outgoing.send(packetFactory.publish(
            qos = AT_LEAST_ONCE,
            topicName = topic,
            payload = payload,
            packetIdentifier = packetIdentifier))

        val puback = incoming
            .filterIsInstance<IPublishAcknowledgment>()
            .filter { it.packetIdentifier == packetIdentifier }
            .take(1).first()
        return@async puback
    }

    override suspend fun close() {
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