package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.controlpacket.IPingResponse
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt.controlpacket.format.ReasonCode
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.SubscribeAcknowledgement
import com.ditchoom.socket.NetworkCapabilities
import com.ditchoom.socket.getNetworkCapabilities
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClientTest {

    private suspend fun prepareConnection(scope: CoroutineScope, useWebsockets: Boolean = false): MqttClient {
        val clientIdPrefix = if (useWebsockets) "testClientWS-" else "testClient-"
        val clientId = clientIdPrefix + Random.nextInt()
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = true, keepAliveSeconds = 1),
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String(clientId)))
        val port = if (useWebsockets) 80u else 1883u
        return MqttClient.connectOnce(scope, connectionRequest, port.toUShort(), useWebsockets = useWebsockets).await()
    }

    @Test
    fun publishAtLeastOnce() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        assertNotNull(client.publishAtLeastOnce("taco", "cheese").await())
        disconnect(client)
    }


    @Test
    fun publishAtLeastOnceWebsocket() = block {
        val client = prepareConnection(this, true)
        assertNotNull(client.publishAtLeastOnce("taco", "cheese").await())
        disconnect(client)
    }

    @Test
    fun publishAtMostOnce() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        client.publishAtMostOnce("taco", "cheese").await()
        disconnect(client)
    }


    @Test
    fun publishAtMostOnceWebsocket() = block {
        val client = prepareConnection(this, true)
        client.publishAtMostOnce("taco", "cheese").await()
        disconnect(client)
    }

    @Test
    fun validateKeepAliveAutomaticCount() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        val expectedPingResponseCount = 1L
        delay(client.connectionRequest.keepAliveTimeoutSeconds.toInt().seconds + 100.milliseconds)
        disconnect(client)
        assertEquals(expectedPingResponseCount, client.pingResponseCount)
    }


    @Test
    fun validateKeepAliveAutomaticCountWebsockets() = block {
        val client = prepareConnection(this, true)
        val expectedPingResponseCount = 1L
        delay(client.connectionRequest.keepAliveTimeoutSeconds.toInt().seconds + 100.milliseconds)
        disconnect(client)
        assertEquals(expectedPingResponseCount, client.pingResponseCount)
    }

    @Test
    fun validateKeepAliveCallback() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        var count = 0L
        val client = prepareConnection(this)
        client.observePongs {
            count++
        }
        val expectedPingResponseCount = 1L
        delay(client.connectionRequest.keepAliveTimeoutSeconds.toInt().seconds + 100.milliseconds)
        disconnect(client)
        assertEquals(expectedPingResponseCount, count)
    }


    @Test
    fun validateKeepAliveCallbackWebsockets() = block {
        var count = 0L
        val client = prepareConnection(this, true)
        client.observePongs {
            count++
        }
        val expectedPingResponseCount = 1L
        delay(client.connectionRequest.keepAliveTimeoutSeconds.toInt().seconds + 100.milliseconds)
        disconnect(client)
        assertEquals(expectedPingResponseCount, count)
    }

    @Test
    fun pingPongWorks() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        assertIs<IPingResponse>(client.ping().await())
        disconnect(client)
    }

    @Test
    fun pingPongWorksWebsockets() = block {
        // browser only supports websockets but does not support ping/pong
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this, true)
        assertIs<IPingResponse>(client.ping().await())
        disconnect(client)
    }

    @Test
    fun subscribePublishAndUnsubscribe() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        subscribePublishAndUnsubscribeImpl(client)
        disconnect(client)
    }

    @Test
    fun subscribePublishAndUnsubscribeWebsockets() = block {
        val client = prepareConnection(this, true)
        subscribePublishAndUnsubscribeImpl(client)
        disconnect(client)
    }

    suspend fun subscribePublishAndUnsubscribeImpl(client: MqttClient) {
        var messagesReceived = 0
        val (_, suback) = client.subscribe("taco") {
            messagesReceived++
        }.await()
        val reasonCode = (suback as SubscribeAcknowledgement).payload.first()
        assertTrue(reasonCode == ReasonCode.GRANTED_QOS_0 || reasonCode == ReasonCode.GRANTED_QOS_1)
        client.publishAtLeastOnce("taco")
        client.unsubscribe("taco").await()
        client.publishAtLeastOnce("taco").await()
        client.publishExactlyOnce("only1").await()

        disconnect(client)

        assertEquals(1, messagesReceived)
    }

    private suspend fun disconnect(client: MqttClient) {
        client.close()
    }
}