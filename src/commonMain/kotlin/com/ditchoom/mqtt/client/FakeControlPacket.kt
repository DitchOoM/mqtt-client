package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.ControlPacketFactory
import com.ditchoom.mqtt.controlpacket.format.fixed.DirectionOfFlow


object FakeControlPacket : ControlPacket {
    override val controlPacketFactory: ControlPacketFactory
        get() = throw UnsupportedOperationException()
    override val controlPacketValue: Byte
        get() = throw UnsupportedOperationException()
    override val direction: DirectionOfFlow
        get() = throw UnsupportedOperationException()
    override val mqttVersion: Byte
        get() = throw UnsupportedOperationException()
}