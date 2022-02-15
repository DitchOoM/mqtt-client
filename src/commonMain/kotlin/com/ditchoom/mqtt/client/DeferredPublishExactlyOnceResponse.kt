package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.controlpacket.IPublishComplete
import com.ditchoom.mqtt.controlpacket.IPublishReceived
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.awaitAll

class DeferredPublishExactlyOnceResponse(
    val pubRecv: Deferred<IPublishReceived>,
    val pubComp: Deferred<IPublishComplete>
) {
    private val all = listOf(pubRecv, pubComp)
    suspend fun await() = all.awaitAll()
}