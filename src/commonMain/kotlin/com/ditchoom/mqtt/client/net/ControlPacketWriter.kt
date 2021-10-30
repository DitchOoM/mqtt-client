package com.ditchoom.mqtt.client.net

import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.buffer.allocateNewBuffer
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.ClientSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ControlPacketWriter private constructor(): SuspendCloseable {
    private val outgoingControlPacketChannel = Channel<Array<out ControlPacket>>()

    suspend fun write(controlPacket: ControlPacket) {
        outgoingControlPacketChannel.send(arrayOf(controlPacket))
    }

    suspend fun write(controlPackets: Array<out ControlPacket>) {
        outgoingControlPacketChannel.send(controlPackets)
    }

    fun listenForPackets(scope: CoroutineScope, socket: ClientSocket, writeTimeout: Duration) {
        scope.launch {
            outgoingControlPacketChannel.consumeEach { controlPackets ->
                val bufferSize = controlPackets.fold(0u) { currentTotalSize, controlPacket ->
                    currentTotalSize + controlPacket.packetSize()
                }
                val writeBuffer = allocateNewBuffer(bufferSize)
                controlPackets.forEach { it.serialize(writeBuffer) }
                socket.write(writeBuffer, writeTimeout)
            }
        }
    }

    override suspend fun close() {
        outgoingControlPacketChannel.close()
    }

    companion object {
        fun build(scope: CoroutineScope, socket: ClientSocket, connectionRequest: IConnectionRequest): ControlPacketWriter {
            val mqttTimeout = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toInt()) * 1.5
            val writer = ControlPacketWriter()
            writer.listenForPackets(scope, socket, mqttTimeout)
            return writer
        }
    }
}