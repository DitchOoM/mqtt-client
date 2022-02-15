package com.ditchoom.mqtt.client;

import com.ditchoom.mqtt.client.IPCMqttClient;

interface MqttClientsChangeCallback {
    void onClientAdded(in IPCMqttClient client);
    void onClientRemoved(in IPCMqttClient client);
}