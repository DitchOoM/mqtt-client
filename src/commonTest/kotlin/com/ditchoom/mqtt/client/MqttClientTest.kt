package com.ditchoom.mqtt.client

import block
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.DisconnectNotification
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttClientTest {
    @Test
    fun connect() = block {
        val connectionRequest = ConnectionRequest(
            payload = ConnectionRequest.Payload(clientId = MqttUtf8String("taco")))
        val client = MqttClient.connect(this, connectionRequest, 1883u).await()
        assertNotNull(client.publishAtLeastOnce("taco", "cheese").await())
        client.send(DisconnectNotification).await()
        client.close()
    }
}