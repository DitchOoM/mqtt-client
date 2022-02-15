package com.ditchoom.mqtt.client

import android.content.ComponentName
import android.content.ServiceConnection
import android.os.IBinder

class MqttServiceConnection(
    private val success: (IPCMqttClientFactory) -> Unit,
    private val onDisconnected: () -> Unit
) : ServiceConnection {
    override fun onServiceConnected(name: ComponentName, service: IBinder) {
        val remoteService = IPCMqttClientFactory.Stub.asInterface(service)
        success(remoteService)
    }

    override fun onServiceDisconnected(name: ComponentName) {
        onDisconnected()
    }
}