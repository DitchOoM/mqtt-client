package com.ditchoom.mqtt.client.net

import block
import com.ditchoom.mqtt.controlpacket.MqttUtf8String
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.DisconnectNotification
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime

@ExperimentalUnsignedTypes
@ExperimentalTime
class MqttSocketSessionTest {

    @Test
    fun connect() = block {
        val connectionRequest = ConnectionRequest(payload = ConnectionRequest.Payload(clientId = MqttUtf8String("taco")))
        val socketSession = MqttSocketSession.openConnection(this, connectionRequest, 1883u, "broker.hivemq.com")
        socketSession.write(DisconnectNotification)
        assertTrue(socketSession.connack.isSuccessful)
    }
}