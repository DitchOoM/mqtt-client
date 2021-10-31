package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt.controlpacket.format.ReasonCode
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.DisconnectNotification
import com.ditchoom.mqtt3.controlpacket.SubscribeAcknowledgement
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlin.test.*
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClientTest {

    private suspend fun prepareConnection(scope: CoroutineScope): MqttClient {
        val connectionRequest = ConnectionRequest(
            ConnectionRequest.VariableHeader(cleanSession = true),
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String("taco")))
        return MqttClient.connect(scope, connectionRequest, 1883u).await()
    }

    @Test
    fun publishAtLeastOnce() = block {
        val client = prepareConnection(this)
        assertNotNull(client.publishAtLeastOnce("taco", "cheese").await())
        disconnect(client)
    }

    @Test
    fun publishAtMostOnce() = block {
        val client = prepareConnection(this)
        client.publishAtMostOnce("taco", "cheese").await()
        disconnect(client)
    }

    @Test
    fun subscribe() = block {
        val client = prepareConnection(this)
        val (sub, suback) = client.subscribe("taco").await()
        val reasonCode = (suback as SubscribeAcknowledgement).payload.first()
        assertTrue(reasonCode == ReasonCode.GRANTED_QOS_0 || reasonCode == ReasonCode.GRANTED_QOS_1)
        disconnect(client)
    }

    private suspend fun disconnect(client: MqttClient) {
        client.close()
    }
}