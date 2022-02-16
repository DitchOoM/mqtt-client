package com.ditchoom.mqtt.client

import android.app.Notification
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.util.Log
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.time.ExperimentalTime


@ExperimentalTime
class MqttService : Service() {

    override fun onBind(intent: Intent): IBinder {
        updateForegroundServiceState(intent)
        return MqttServiceClientFactoryBinder(this)
    }

    override fun onStartCommand(intent: Intent, flags: Int, startId: Int): Int {
        updateForegroundServiceState(intent)
        return START_STICKY
    }

    private fun updateForegroundServiceState(intent: Intent) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.ECLAIR) return
        val shouldForceForegroundStateChange = intent.getBooleanExtra(SHOULD_FORCE_FOREGROUND_CHANGE, false)
        val id = intent.getIntExtra(FOREGROUND_SERVICE_ID_EXTRA, 0)
        val notification = intent.getParcelableExtra<Notification>(FOREGROUND_SERVICE_EXTRA)
        if (shouldForceForegroundStateChange) {
            if (notification != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q &&
                    intent.hasExtra(FOREGROUND_SERVICE_TYPE_EXTRA)) {
                    val type = intent.getIntExtra(FOREGROUND_SERVICE_TYPE_EXTRA, Int.MIN_VALUE)
                    startForeground(id, notification, type)
                } else {
                    startForeground(id, notification)
                }
            } else {
                stopForeground(true)
            }
        }

    }

    companion object {

        private const val FOREGROUND_SERVICE_EXTRA = "foregroundServiceNotification"
        private const val SHOULD_FORCE_FOREGROUND_CHANGE = "forceForegroundChange"
        private const val FOREGROUND_SERVICE_TYPE_EXTRA = "foregroundServiceNotificationType"
        private const val FOREGROUND_SERVICE_ID_EXTRA = "foregroundServiceNotificationID"
        private val serviceJob = Job()
        internal val processScope = CoroutineScope(Dispatchers.Default + serviceJob)

        suspend fun getFactory(context: Context, shouldForceForegroundStateChange: Boolean = false, foregroundNotificationId: Int = 0, foregroundNotification: Notification? = null, foregroundNotificationType: Int = 0) = suspendCancellableCoroutine<IPCMqttClientFactory> {
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
            val intent = Intent(context, MqttService::class.java)
            intent.putExtra(SHOULD_FORCE_FOREGROUND_CHANGE, shouldForceForegroundStateChange)
            intent.putExtra(FOREGROUND_SERVICE_ID_EXTRA, foregroundNotificationId)
            intent.putExtra(FOREGROUND_SERVICE_TYPE_EXTRA, foregroundNotificationType)
            if (foregroundNotification != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                intent.putExtra(FOREGROUND_SERVICE_EXTRA, foregroundNotification)
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
            context.bindService(
                intent,
                mqttServiceConnection,
                BIND_AUTO_CREATE
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