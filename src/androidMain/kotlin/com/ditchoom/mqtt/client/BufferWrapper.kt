package com.ditchoom.mqtt.client

import android.os.Parcel
import android.os.Parcelable
import com.ditchoom.buffer.JvmBuffer
import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.buffer.ParcelableSharedMemoryBuffer


class BufferWrapper(val buffer: ParcelablePlatformBuffer) : Parcelable {
    override fun describeContents(): Int {
        return buffer.describeContents()
    }

    override fun writeToParcel(dest: Parcel, flags: Int) {
        dest.writeByte(if (buffer is ParcelableSharedMemoryBuffer) 1 else 0)
        buffer.writeToParcel(dest, flags)
    }

    companion object CREATOR : Parcelable.Creator<BufferWrapper> {
        override fun createFromParcel(parcel: Parcel): BufferWrapper {
            val isParcelableSharedMemoryBuffer = parcel.readByte() == 1.toByte()
            val buffer = if (isParcelableSharedMemoryBuffer) {
                ParcelableSharedMemoryBuffer.CREATOR.createFromParcel(parcel)
            } else {
                JvmBuffer.CREATOR.createFromParcel(parcel)
            }
            return BufferWrapper(buffer)
        }

        override fun newArray(size: Int): Array<BufferWrapper?> = arrayOfNulls(size)
    }
}