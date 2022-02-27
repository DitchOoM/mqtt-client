@file:OptIn(ExperimentalTime::class)

package com.ditchoom.mqtt.client

import android.app.Service
import android.net.ConnectivityManager
import android.os.Bundle
import android.os.Parcelable
import android.util.Log
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

internal class MqttServiceClientFactoryBinder(private val service: Service) : IPCMqttClientFactory.Stub() {

    private val clientChangeCallbacks = mutableListOf<MqttClientsChangeCallback>()
    private val connectivityManager = service.getSystemService(Service.CONNECTIVITY_SERVICE) as ConnectivityManager
    private val clientMapping = mutableMapOf<IPCMqttClient, ReconnectingMqttClient>()

    override fun openConnection(
        port: Int,
        host: String,
        useWebsockets: Boolean,
        connectionRequestBundle: ControlPacketWrapper,
        bundle: Bundle
    ): IPCMqttClient {
        bundle.classLoader = MqttService.javaClass.classLoader
        val parcelable = bundle.getParcelable<Parcelable>(ParcelablePersistenceFactory.BUNDLE_KEY)!!
        val persistence = (parcelable as ParcelablePersistenceFactory).newPersistence(service)
        val connectionRequest = connectionRequestBundle.external as IConnectionRequest

        val ipcScope = MqttService.processScope +
                CoroutineName("IPCClient ${connectionRequest.clientIdentifier}@$host:$port")
        val ipcClient = IPCMqttClientImpl(ipcScope + Dispatchers.Main)
        ipcScope.launch {
            val (reconnectingClient, _) = ReconnectingMqttClient.stayConnected(
                MqttService.processScope,
                persistence,
                connectionRequest,
                port.toUShort(),
                host,
                useWebsockets,
                30.seconds,
                { 1.seconds },
                { incoming ->
                    val wrapped = ControlPacketWrapper(incoming)
                    Log.i("RAHUL", "Incoming $incoming")
                    ipcClient.incomingCb.forEach { it.onSuccess(wrapped) }
                }) { outgoing ->
                val wrapped = ControlPacketWrapper(outgoing)
                Log.i("RAHUL", "Outgoing $outgoing")
                ipcClient.outgoingCb.forEach { it.onSuccess(wrapped) }
            }
            ipcClient.setClient(reconnectingClient)
            MqttClientNetworkObserver.registerNetworkCallback(
                MqttService.processScope,
                connectivityManager,
                reconnectingClient
            )
            clientMapping[ipcClient] = reconnectingClient
            clientChangeCallbacks.forEach { it.onClientAdded(ipcClient) }
        }

        return ipcClient
    }

    override fun killConnection(client: IPCMqttClient) {
        val reconnectingClient = clientMapping.remove(client) ?: return
        MqttService.processScope.launch {
            reconnectingClient.sendDisconnect()
            reconnectingClient.close()
            clientChangeCallbacks.forEach { it.onClientRemoved(client) }
        }
    }

    override fun addClientsChangeCallback(cb: MqttClientsChangeCallback) {
        clientChangeCallbacks += cb
    }

    override fun getAllClients(): List<IPCMqttClient> {
        return clientMapping.keys.toList()
    }

    override fun removeClientsChangeCallback(cb: MqttClientsChangeCallback) {
        clientChangeCallbacks -= cb
    }
}