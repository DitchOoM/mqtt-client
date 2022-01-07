package com.ditchoom.mqtt.client

import com.ditchoom.buffer.SuspendCloseable
import kotlinx.coroutines.channels.Channel


/**
 * Suspends a coroutine when awaitCompletion() is called and continues when completed() is called
 */
internal class SuspendableWaiter(private val channel: Channel<Unit> = Channel()) : SuspendCloseable {

    var count = 0
    suspend fun awaitCompletion() {
        count++
        channel.receive()
//        println("await completion $channel")
    }

    fun completed() {
        channel.trySend(Unit)
//        println("completed $channel")
    }

    override fun toString(): String {
        return "SuspendableWaiter${hashCode()}( $count)"
    }

    override suspend fun close() {

    }
}