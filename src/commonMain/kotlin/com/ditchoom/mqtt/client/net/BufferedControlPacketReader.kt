@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client.net

import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.mqtt.MalformedInvalidVariableByteInteger
import com.ditchoom.mqtt.controlpacket.ControlPacketFactory
import com.ditchoom.socket.ClientSocket
import com.ditchoom.socket.SuspendingSocketInputStream
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.flow.flow
import kotlin.experimental.and
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class BufferedControlPacketReader private constructor(
    private val clientSocket: ClientSocket,
    private val stream: SuspendingSocketInputStream,
    private val controlPacketFactory: ControlPacketFactory,
): SuspendCloseable {
    suspend fun readControlPackets() = flow {
        try {
            while (clientSocket.isOpen()) {
                val byte1 = stream.readUnsignedByte()
                val remainingLength = readVariableByteInteger()
                val remainingPayload = stream.sizedReadBuffer(remainingLength.toInt())
                emit(controlPacketFactory.from(remainingPayload, byte1, remainingLength))
            }
        } catch (e: ClosedReceiveChannelException) {

        }
        close()
    }

    private suspend fun readVariableByteInteger(): UInt {
        var digit: Byte
        var value = 0L
        var multiplier = 1L
        var count = 0L
        try {
            do {
                digit = stream.readByte()
                count++
                value += (digit and 0x7F).toLong() * multiplier
                multiplier *= 128
            } while ((digit and 0x80.toByte()).toInt() != 0)
        } catch (e: Exception) {
            throw MalformedInvalidVariableByteInteger(value.toUInt())
        }
        val variableByteIntMax = 268435455L
        if (value < 0 || value > variableByteIntMax) {
            throw MalformedInvalidVariableByteInteger(value.toUInt())
        }
        return value.toUInt()
    }

    override suspend fun close() {
        stream.close()
    }

    companion object {
        fun build(
            scope: CoroutineScope,
            socket: ClientSocket,
            socketReadTimeout: Duration,
            controlPacketFactory: ControlPacketFactory,
        ) = BufferedControlPacketReader(socket, socket.suspendingInputStream(scope, socketReadTimeout), controlPacketFactory)
    }
}