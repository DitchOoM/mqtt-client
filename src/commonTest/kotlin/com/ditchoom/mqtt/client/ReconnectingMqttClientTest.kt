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
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ReconnectingMqttClientTest {
    private val keepAliveSeconds = 1

    private fun prepareConnection(
        scope: CoroutineScope,
        useWebsockets: Boolean = false,
        cleanSession: Boolean = true
    ): Pair<ReconnectingMqttClient, CancelConnection> {
        val clientIdPrefix = if (useWebsockets) "testClientWS-" else "testClient-"
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = cleanSession, keepAliveSeconds = keepAliveSeconds),
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String("$clientIdPrefix${Random.nextInt()}"))
        )
        val port = if (useWebsockets) 80u else 1883u
        @Suppress("UNCHECKED_CAST")
        return ReconnectingMqttClient.stayConnected(
            scope,
            connectionRequest,
            port.toUShort(),
            useWebsockets = useWebsockets
        ) as Pair<ReconnectingMqttClient, CancelConnection>
    }

    @Test
    fun messageDeque() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val (client, cancellation) = prepareConnection(this, useWebsockets = false, cleanSession = false)
        messageDequeSuspend(client, cancellation)
    }

    @Test
    fun messageDequeWebsockets() = block {
        val (client, cancellation) = prepareConnection(this, useWebsockets = true, cleanSession = false)
        messageDequeSuspend(client, cancellation)
    }

    suspend fun messageDequeSuspend(client: ReconnectingMqttClient, cancellation: CancelConnection) {
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        client.pauseReconnects()
        firstClientSession.waitUntilDisconnectAsync()
        delay(3000)
        val pubAsync = client.publishAtLeastOnce("reconnect")
        assertFalse(client.isConnected())
        client.resumeReconnects()
        client.awaitClientConnection()
        assertNotNull(pubAsync.await())
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }


    @Test
    fun reconnectsOnce() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val (client, cancellation) = prepareConnection(this)
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        firstClientSession.waitUntilDisconnectAsync()
        client.awaitClientConnection()
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }


    @Test
    fun reconnectsOnceWebsockets() = block {
        val (client, cancellation) = prepareConnection(this, true)
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        firstClientSession.waitUntilDisconnectAsync()
        client.awaitClientConnection()
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }

    private suspend fun disconnect(client: IMqttClient) {
        client.close()
    }
}