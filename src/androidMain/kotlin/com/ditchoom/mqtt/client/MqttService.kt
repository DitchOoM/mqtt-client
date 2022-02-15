package com.ditchoom.mqtt.client

import android.R
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import kotlinx.coroutines.*
import kotlin.coroutines.resume
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime


@ExperimentalTime
class MqttService : Service() {

    override fun onBind(intent: Intent?) = object : IPCMqttClientFactory.Stub() {
        private val clientChangeCallbacks = mutableListOf<MqttClientsChangeCallback>()
        val connectivityManager = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager
        private val clientMapping = mutableMapOf<IPCMqttClient, ReconnectingMqttClient>()

        override fun openConnection(
            port: Int,
            host: String,
            useWebsockets: Boolean,
            connectionRequestBundle: ControlPacketWrapper
        ): IPCMqttClient {
            val connectionRequest = connectionRequestBundle.external as IConnectionRequest

            val ipcScope = processScope +
                    CoroutineName("IPCClient ${connectionRequest.clientIdentifier}@$host:$port")
            val ipcClient = IPCMqttClientImpl(ipcScope + Dispatchers.Main)
            ipcScope.launch {
                val (reconnectingClient, _) = ReconnectingMqttClient.stayConnected(
                    processScope,
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
                    processScope,
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
            processScope.launch {
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

    override fun onCreate() {
        super.onCreate()

        prepareChannel(this, "mqtt", NotificationManagerCompat.IMPORTANCE_MIN)
        startForeground(
            123, NotificationCompat.Builder(this, "mqtt")
                .setTicker("Mqtt Running")
                .setSmallIcon(R.color.black)
                //.setLargeIcon(Bitmap.createScaledBitmap(icon, 128, 128, false))
                .setOngoing(true)
                .setContentText("running").build()
        )
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {

        return START_STICKY
    }

    private fun prepareChannel(context: Context, id: String, importance: Int) {
        val appName = ("mqtt")
        val description = ("mqtt")
        val nm = context.getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        var nChannel = nm.getNotificationChannel(id)
        if (nChannel == null) {
            nChannel = NotificationChannel(id, appName, importance)
            nChannel.description = description
            nm.createNotificationChannel(nChannel)
        }
    }

    companion object {


        private val serviceJob = Job()
        internal val processScope = CoroutineScope(Dispatchers.Default + serviceJob)

        suspend fun getFactory(context: Context) = suspendCancellableCoroutine<IPCMqttClientFactory> {
            var hasResumed = false
            val mqttServiceConnection = MqttServiceConnection({ clientFactory ->
                if (!hasResumed) {
                    hasResumed = true
                    it.resume(clientFactory)
                }
            }) {
                Log.i("MqttService", "Lost connection to remote")
                if (!hasResumed) {
                    hasResumed = true
                    it.cancel()
                }
            }

            context.startForegroundService(Intent(context, MqttService::class.java))
            context.bindService(
                Intent(context, MqttService::class.java),
                mqttServiceConnection,
                Context.BIND_AUTO_CREATE
            )
        }

        fun connect(
            context: Context,
            clientFactory: IPCMqttClientFactory,
            port: Int,
            host: String,
            connectionRequest: IConnectionRequest,
            useWebSockets: Boolean,
        ): IPCMqttClient {
            return clientFactory.openConnection(port, host, useWebSockets, ControlPacketWrapper(connectionRequest))
        }
    }
}