@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.ControlPacketFactory
import com.ditchoom.mqtt.controlpacket.format.fixed.DirectionOfFlow

interface Persistence {
    suspend fun getAllPendingIds(): Set<Int>
    suspend fun nextPacketIdentifier(): Int
    suspend fun save(packetIdentifier: Int, data: ControlPacket)
    suspend fun delete(id: Int)
    suspend fun readNextControlPacketOrNull(): ControlPacket?
}

class InMemoryPersistence: Persistence {
    private val queue = HashMap<Int, ControlPacket>()
    override suspend fun getAllPendingIds() = queue.keys

    override suspend fun nextPacketIdentifier(): Int {
        for (i in 1..65535) {
            if (!queue.containsKey(i)) {
                queue[i] = FakeControlPacket
                return i
            }
        }
        throw IllegalStateException("Too many queued messages. Can't get a new packet identifier.")
    }

    override suspend fun save(packetIdentifier: Int, data: ControlPacket) {
        queue[packetIdentifier] = data
    }

    override suspend fun delete(id: Int) {
        queue.remove(id)
    }

    override suspend fun readNextControlPacketOrNull(): ControlPacket? {
        val firstKey = queue.keys.firstOrNull() ?: return null
        return queue[firstKey]
    }

    object FakeControlPacket: ControlPacket {
        override val controlPacketFactory: ControlPacketFactory
            get() = throw UnsupportedOperationException()
        override val controlPacketValue: Byte
            get() = throw UnsupportedOperationException()
        override val direction: DirectionOfFlow
            get() = throw UnsupportedOperationException()
        override val mqttVersion: Byte
            get() = throw UnsupportedOperationException()
    }

}