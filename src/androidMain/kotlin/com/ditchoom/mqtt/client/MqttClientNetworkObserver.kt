@file:OptIn(ExperimentalTime::class)

package com.ditchoom.mqtt.client

import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlin.time.ExperimentalTime

class MqttClientNetworkObserver private constructor(
    private val scope: CoroutineScope,
    private val connectivityManager: ConnectivityManager,
    private val client: ReconnectingMqttClient,
) : ConnectivityManager.NetworkCallback() {

    private val availableNetworks = mutableSetOf<Network>()

    override fun onAvailable(network: Network) {
        availableNetworks += network
        // allow reconnects, reset any timers
        if (availableNetworks.size == 1) {
            client.resumeReconnects()
        }
    }

    override fun onLosing(network: Network, maxMsToLive: Int) {
        // start the reconnection process on a different network, regardless of current state
        if (client.isConnected()) {
            scope.launch {
                client.sendDisconnect()
            }
        }
    }

    override fun onLost(network: Network) {
        client.ping()
        availableNetworks -= network
        // if available networks is empty, stop reconnects
        if (availableNetworks.isEmpty()) {
            client.pauseReconnects()
        }
    }

    companion object {
        fun registerNetworkCallback(
            scope: CoroutineScope,
            connectivityManager: ConnectivityManager,
            client: ReconnectingMqttClient
        ): ConnectivityManager.NetworkCallback {
            val observer = MqttClientNetworkObserver(scope, connectivityManager, client)
            connectivityManager.registerNetworkCallback(NetworkRequest.Builder().build(), observer)
            return observer
        }
    }
}