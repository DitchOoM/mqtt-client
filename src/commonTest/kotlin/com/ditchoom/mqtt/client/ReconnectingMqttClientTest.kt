package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.client.ReconnectingMqttClient.Companion.CancelConnection
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.socket.NetworkCapabilities
import com.ditchoom.socket.getNetworkCapabilities
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

@ExperimentalTime
class ReconnectingMqttClientTest {
    private val keepAliveSeconds = 1

    private fun prepareConnection(scope: CoroutineScope, useWebsockets: Boolean = false): Pair<ReconnectingMqttClient, CancelConnection> {
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = true, keepAliveSeconds = keepAliveSeconds),
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String("testClient${Random.nextInt()}")))
        val port = if (useWebsockets) 80u else 1883u
        @Suppress("UNCHECKED_CAST")
        return ReconnectingMqttClient.stayConnected(scope, connectionRequest, port.toUShort(), useWebsockets = useWebsockets) as Pair<ReconnectingMqttClient, CancelConnection>
    }

    @Test
    fun reconnectsOnce() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val start = TimeSource.Monotonic.markNow()
        val (client, cancellation) = prepareConnection(this)
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        println("waitUntilDisconnectAsync ${start.elapsedNow()}")
        firstClientSession.waitUntilDisconnectAsync()
        println("disconnected in ${start.elapsedNow()}, reconnecting")
        client.awaitClientConnection()
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }


    @Test
    fun reconnectsOnceWebsockets() = block {
        val start = TimeSource.Monotonic.markNow()
        val (client, cancellation) = prepareConnection(this, true)
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        println("waitUntilDisconnectAsync ${start.elapsedNow()}")
        firstClientSession.waitUntilDisconnectAsync()
        println("disconnected in ${start.elapsedNow()}, reconnecting")
        client.awaitClientConnection()
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }

    private suspend fun disconnect(client: IMqttClient) {
        client.close()
    }
}