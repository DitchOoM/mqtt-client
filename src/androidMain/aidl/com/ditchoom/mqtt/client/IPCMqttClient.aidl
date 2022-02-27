package com.ditchoom.mqtt.client;

import android.os.SharedMemory;
import com.ditchoom.mqtt.client.DataCallback;
import com.ditchoom.mqtt.client.ControlPacketWrapper;
import com.ditchoom.mqtt.client.MqttConnectionStateCallback;

interface IPCMqttClient {
    Bundle getConnectionState();
    ControlPacketWrapper getConnectionRequest();
    String getHost();
    int getPort();
    boolean usingWebsockets();
    int remoteHashCode();
    void addConnectionCallback(in MqttConnectionStateCallback callback);
    void removeConnectionCallback(in MqttConnectionStateCallback callback);

    void publishAtMostOnce(String topic, boolean retain, in @nullable Bundle payload);
    void publishAtLeastOnce(String topic, boolean retain, in @nullable Bundle payload, boolean persist, in DataCallback callback);
    void publishExactlyOnce(String topic, boolean retain, in @nullable Bundle payload, boolean persist, in DataCallback callbackRecv, in DataCallback callbackComp);

    void subscribe(String topicFilter, boolean persist, byte qos, in @nullable DataCallback callback);
    void unsubscribe(String topicFilter, boolean persist, in @nullable DataCallback callback);

    void observe(in String topicFilter, in DataCallback callback);

    void registerIncomingMessageListener(in DataCallback callback);
    void unregisterIncomingMessageListener(in DataCallback callback);
    void registerOutgoingMessageListener(in DataCallback callback);
    void unregisterOutgoingMessageListener(in DataCallback callback);

    void ping(in DataCallback callback);
    void pauseReconnects();
    void resumeReconnects();
    void close();
    String getInfoString();
}