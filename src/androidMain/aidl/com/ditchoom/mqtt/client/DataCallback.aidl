package com.ditchoom.mqtt.client;

import android.os.SharedMemory;
import com.ditchoom.mqtt.client.ControlPacketWrapper;

interface DataCallback {
    void onSuccess(in ControlPacketWrapper data);
}