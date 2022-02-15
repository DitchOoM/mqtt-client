package com.ditchoom.mqtt.client;

import com.ditchoom.mqtt.client.IPCMqttClient;

interface MqttConnectionStateCallback {
    void onChange(in Bundle state);
}