package com.ditchoom.mqtt.client;

import android.os.SharedMemory;
import com.ditchoom.mqtt.client.MqttConnectionStateCallback;
import com.ditchoom.mqtt.client.MqttClientsChangeCallback;
import com.ditchoom.mqtt.client.ControlPacketWrapper;
import com.ditchoom.mqtt.client.IPCMqttClient;

interface IPCMqttClientFactory {
    IPCMqttClient openConnection(
     int port,
     in String host,
     boolean useWebsockets,
     in ControlPacketWrapper connectionRequest,
     in Bundle persistence
    );

    void killConnection(in IPCMqttClient client);

    void addClientsChangeCallback(in MqttClientsChangeCallback cb);
    List getAllClients();
    void removeClientsChangeCallback(in MqttClientsChangeCallback cb);
}