@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.ReadBuffer
import com.ditchoom.buffer.allocateNewBuffer
import com.ditchoom.data.Reader
import com.ditchoom.mqtt.MalformedInvalidVariableByteInteger
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.ControlPacketFactory
import com.ditchoom.socket.SuspendingSocketInputStream
import kotlin.experimental.and
import kotlin.time.Duration
import kotlin.time.ExperimentalTime


@ExperimentalTime
class BufferedControlPacketReader(
    private val factory: ControlPacketFactory,
    readTimeout: Duration,
    private val dataReader: Reader<ReadBuffer>,
) : Reader<ControlPacket> {
    private val inputStream = SuspendingSocketInputStream(readTimeout, dataReader)

    override fun isOpen() = dataReader.isOpen()

    suspend fun readControlPacket(): ControlPacket {
        val byte1 = inputStream.readUnsignedByte()
        val remainingLength = readVariableByteInteger()
        return factory.from(inputStream.sizedReadBuffer(remainingLength.toInt()), byte1, remainingLength)
    }

    override suspend fun readData(timeout: Duration) = readControlPacket()

    private suspend fun readVariableByteInteger(): UInt {
        var digit: Byte
        var value = 0L
        var multiplier = 1L
        var count = 0L
        try {
            do {
                digit = inputStream.readByte()
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
}

fun ControlPacket.toBuffer() = arrayOf(this).toBuffer()

fun Array<out ControlPacket>.toBuffer(): PlatformBuffer {
    val packetSize = fold(0u) { currentPacketSize, controlPacket ->
        currentPacketSize + controlPacket.packetSize()
    }
    return fold(allocateNewBuffer(packetSize)) { buffer, controlPacket ->
        controlPacket.serialize(buffer)
        buffer
    }
}
