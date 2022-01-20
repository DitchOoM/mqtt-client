@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.buffer.allocateNewBuffer
import com.ditchoom.mqtt.controlpacket.ControlPacket

fun ControlPacket.toBuffer() = arrayOf(this).toBuffer()

fun Array<out ControlPacket>.toBuffer(): ParcelablePlatformBuffer {
    val packetSize = fold(0u) { currentPacketSize, controlPacket ->
        currentPacketSize + controlPacket.packetSize()
    }
    return fold(allocateNewBuffer(packetSize)) { buffer, controlPacket ->
        controlPacket.serialize(buffer)
        buffer
    }
}