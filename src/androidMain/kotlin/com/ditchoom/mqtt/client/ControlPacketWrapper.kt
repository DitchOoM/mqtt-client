@file:OptIn(ExperimentalUnsignedTypes::class)

package com.ditchoom.mqtt.client

import android.os.Parcel
import android.os.Parcelable
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt3.controlpacket.ControlPacketV4
import com.ditchoom.mqtt5.controlpacket.ControlPacketV5
import kotlinx.parcelize.Parceler
import kotlinx.parcelize.Parcelize


@Parcelize
class ControlPacketWrapper(
    val external: ControlPacket,
    val time: Long = System.currentTimeMillis(),
) : Parcelable {
    private companion object : Parceler<ControlPacketWrapper> {
        override fun create(parcel: Parcel): ControlPacketWrapper {
            val time = parcel.readLong()
            val version = parcel.readByte().toInt()
            val bundle = parcel.readBundle(Companion::class.java.classLoader)!!
            val buffer = bundle.toBufferOrNull()!!
            val packet = if (version == 4) {
                ControlPacketV4.Companion.from(buffer)
            } else {
                ControlPacketV5.Companion.from(buffer)
            }
            return ControlPacketWrapper(packet, time)
        }

        override fun ControlPacketWrapper.write(parcel: Parcel, flags: Int) {
            parcel.writeLong(time)
            parcel.writeByte(external.mqttVersion)
            val b = external.toBuffer()
            b.resetForRead()
            parcel.writeBundle(b.toBundle())
            b.toString()
        }

    }
}