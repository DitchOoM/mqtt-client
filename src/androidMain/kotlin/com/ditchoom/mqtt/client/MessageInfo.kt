package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.controlpacket.ControlPacket

class MessageInfo(val packet: ControlPacket, val time: Long, val didRecv: Boolean)