@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client.net

import com.ditchoom.buffer.allocateNewBuffer
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.ClientSocket
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

@ExperimentalTime
class ControlPacketWriter private constructor(
    private val socket: ClientSocket,
    private val writeTimeout: Duration,
) {

    val now = TimeSource.Monotonic.markNow()
    suspend fun write(controlPacket: ControlPacket) {
        write(arrayOf(controlPacket))
    }

    suspend fun write(controlPackets: Array<out ControlPacket>) {
        val bufferSize = controlPackets.fold(0u) { currentTotalSize, controlPacket ->
            currentTotalSize + controlPacket.packetSize()
        }
        val writeBuffer = allocateNewBuffer(bufferSize)
        controlPackets.forEach { it.serialize(writeBuffer) }
        socket.write(writeBuffer, writeTimeout)
    }

    companion object {
        fun build(
            socket: ClientSocket,
            connectionRequest: IConnectionRequest
        ): ControlPacketWriter {
            val mqttTimeout = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toInt()) * 1.5
            return ControlPacketWriter(socket, mqttTimeout)
        }
    }
}