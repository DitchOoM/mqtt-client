package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.controlpacket.IPingResponse
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt.controlpacket.format.ReasonCode
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.DisconnectNotification
import com.ditchoom.mqtt3.controlpacket.SubscribeAcknowledgement
import com.ditchoom.socket.NetworkCapabilities
import com.ditchoom.socket.getNetworkCapabilities
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.yield
import kotlin.test.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClientTest {

    private suspend fun prepareConnection(scope: CoroutineScope, useWebsockets: Boolean = false): MqttClient {
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = true, keepAliveSeconds = 1),
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String("testClient")))
        val port = if (useWebsockets) 80u else 1883u
        return MqttClient.connect(scope, connectionRequest, port.toUShort(), useWebsockets = useWebsockets).await()
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
    fun validateKeepAliveAutomaticCount() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        val expectedPingResponseCount = 1L
        delay(seconds(client.connectionRequest.keepAliveTimeoutSeconds.toInt()) + milliseconds(100))
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
        delay(seconds(client.connectionRequest.keepAliveTimeoutSeconds.toInt()) + milliseconds(100))
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
    fun subscribePublishAndUnsubscribe() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val client = prepareConnection(this)
        var messagesReceived = 0
        val (sub, suback) = client.subscribe("taco") {
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