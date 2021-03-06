package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.client.ReconnectingMqttClient.Companion.CancelConnection
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.socket.NetworkCapabilities
import com.ditchoom.socket.getNetworkCapabilities
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ReconnectingMqttClientTest {
    private val keepAliveSeconds = 1

    private fun prepareConnection(
        scope: CoroutineScope,
        useWebsockets: Boolean = false,
        cleanSession: Boolean = true,
        hostname: String = "localhost",
        port: Int? = null
    ): Pair<ReconnectingMqttClient, CancelConnection> {
        val clientIdPrefix = if (useWebsockets) "testClientWS-" else "testClient-"
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = cleanSession, keepAliveSeconds = keepAliveSeconds),
            payload = ConnectionRequest.Payload(clientId = "$clientIdPrefix${Random.nextInt()}")
        )
        val portLocal = port?.toUShort() ?: if (useWebsockets) 80u else 1883u
        return ReconnectingMqttClient.stayConnected(
            scope,
            InMemoryPersistence(),
            connectionRequest,
            portLocal,
            hostname,
            useWebsockets = useWebsockets,
            connectTimeout = 3.seconds,
        )
    }

    @Test
    fun connectTimeoutWorks() = block {
        val (client, cancellation) = prepareConnection(
            this,
            useWebsockets = false,
            cleanSession = false,
            hostname = "example.com",
            port = 3
        )
        delay(600.milliseconds)
        assertTrue(client.reconnectionCount > 0uL)
        client.stayConnectedJob.cancel()
    }

    @Test
    fun messageDeque() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val (client, cancellation) = prepareConnection(this, useWebsockets = false, cleanSession = false)
        messageDequeSuspend(this, client, cancellation)
    }

    @Test
    fun messageDequeWebsockets() = block {
        val (client, cancellation) = prepareConnection(this, useWebsockets = true, cleanSession = false)
        messageDequeSuspend(this, client, cancellation)
    }

    suspend fun messageDequeSuspend(
        scope: CoroutineScope,
        client: ReconnectingMqttClient,
        cancellation: CancelConnection
    ) {
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        client.pauseReconnects()
        scope.launch { client.sendDisconnect() }
        firstClientSession.waitUntilDisconnectAsync()
        val pubAsync = client.publishAtLeastOnce("reconnect")
        assertFalse(client.isConnected())
        client.resumeReconnects()
        client.awaitClientConnection()
        assertNotNull(pubAsync.await())
        // We should have atleast reconnected once by now
        assertTrue(client.reconnectionCount > 0uL)
    }


    @Test
    fun reconnectsOnce() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val (client, cancellation) = prepareConnection(this)
        client.maxReconnectionCount = 1uL
        // cancel the keep alive timer. the server will disconnect the client if it exceeds 1.5x the keep alive timer
        cancellation.ignoreKeepAlive()
        val firstClientSession = client.awaitClientConnection()
        launch { client.sendDisconnect() }
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
        launch { client.sendDisconnect() }
        firstClientSession.waitUntilDisconnectAsync()
        client.awaitClientConnection()
        // We should have atleast reconnected once by now
        assertEquals(1uL, client.reconnectionCount)
    }

    private suspend fun disconnect(client: IMqttClient) {
        client.close()
    }
}