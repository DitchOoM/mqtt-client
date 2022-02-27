@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.controlpacket.*
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

interface Persistence {
    suspend fun nextPacketIdentifier(persist: Boolean = true): Int
    suspend fun save(packetIdentifier: Int, data: ControlPacket)
    suspend fun delete(id: Int)
    suspend fun queuedControlPackets(): Flow<Pair<Int, ControlPacket>>
}

class InMemoryPersistence : Persistence {
    private val queue = HashMap<Int, ControlPacket>()
    private val count = atomic(1)

    override suspend fun nextPacketIdentifier(persist: Boolean): Int {
        var nextId = count.incrementAndGet() % UShort.MAX_VALUE.toInt()
        while (queue.containsKey(nextId)) {
            nextId = count.incrementAndGet() % UShort.MAX_VALUE.toInt()
        }
        if (persist) {
            queue[nextId] = FakeControlPacket
        }
        return nextId
    }

    override suspend fun save(packetIdentifier: Int, data: ControlPacket) {
        queue[packetIdentifier] = data
    }

    override suspend fun delete(id: Int) {
        queue.remove(id)
    }

    override suspend fun queuedControlPackets() = flow {
        queue.forEach { (_, packet) ->
            val item = when (packet) {
                is IPublishMessage -> packet.packetIdentifier?.let { Pair(it, packet) }
                is ISubscribeRequest -> Pair(packet.packetIdentifier, packet)
                is IUnsubscribeRequest -> Pair(packet.packetIdentifier, packet)
                is IPublishRelease -> Pair(packet.packetIdentifier, packet)
                else -> null
            }
            if (item != null) {
                emit(item)
            }
        }
    }
}