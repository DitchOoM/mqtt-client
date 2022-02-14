package com.ditchoom.mqtt.client.net

import block
import com.ditchoom.mqtt.client.MqttSocketSession
import com.ditchoom.mqtt.controlpacket.QualityOfService
import com.ditchoom.mqtt3.controlpacket.ConnectionRequest
import com.ditchoom.mqtt3.controlpacket.DisconnectNotification
import com.ditchoom.socket.NetworkCapabilities
import com.ditchoom.socket.getNetworkCapabilities
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalUnsignedTypes
@ExperimentalTime
class MqttSocketSessionTest {

    @Test
    fun connect() = block {
        if (getNetworkCapabilities() != NetworkCapabilities.FULL_SOCKET_ACCESS) return@block
        val connectionRequest =
            ConnectionRequest(payload = ConnectionRequest.Payload(clientId = "taco"))
        val socketSession = MqttSocketSession.openConnection(
            connectionRequest,
            1883u,
            "localhost",
            false,
            connectionRequest.keepAliveTimeoutSeconds.toInt().seconds * 1.5,
            null,
            null
        )
        assertTrue(socketSession.connectionAcknowledgement.isSuccessful)
        socketSession.write(
            connectionRequest.controlPacketFactory.publish(
                topicName = "testtt", qos = QualityOfService.AT_MOST_ONCE
            )
        )

        socketSession.write(DisconnectNotification)
    }

    @Test
    fun connectWebsockets() = block {
        val connectionRequest =
            ConnectionRequest(payload = ConnectionRequest.Payload(clientId = "taco"))
        val socketSession = MqttSocketSession.openConnection(
            connectionRequest,
            80u,
            "localhost",
            true,
            connectionRequest.keepAliveTimeoutSeconds.toInt().seconds * 1.5,
            null,
            null)
        assertTrue(socketSession.connectionAcknowledgement.isSuccessful)
        socketSession.write(
            connectionRequest.controlPacketFactory.publish(
                topicName = "testtt", qos = QualityOfService.AT_MOST_ONCE
            )
        )

        socketSession.write(DisconnectNotification)
    }
}