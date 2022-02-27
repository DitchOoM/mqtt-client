@file:OptIn(ExperimentalTime::class)

package com.ditchoom.mqtt.client

import android.os.Bundle
import android.os.Parcelable
import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.mqtt.controlpacket.QualityOfService
import com.ditchoom.mqtt.topic.Filter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlin.time.ExperimentalTime

@ExperimentalTime
class IPCMqttClientImpl(private val scope: CoroutineScope) : IPCMqttClient.Stub() {
    private val callbacks = mutableSetOf<MqttConnectionStateCallback>()
    val incomingCb = mutableSetOf<DataCallback>()
    val outgoingCb = mutableSetOf<DataCallback>()
    lateinit var client: ReconnectingMqttClient
    suspend fun setClient(client: ReconnectingMqttClient) {
        this.client = client
        scope.launch {
            val sharedFlow: SharedFlow<Parcelable> = client.connectionStateObservers
            sharedFlow.collect { connectionState ->
                val b = Bundle()
                b.putParcelable("state", connectionState)
                callbacks.forEach { it.onChange(b) }
            }
        }
    }

    override fun getConnectionState(): Bundle {
        val b = Bundle()
        b.putParcelable("state", client.currentConnectionState)
        return b
    }
    override fun remoteHashCode() = hashCode()
    override fun getConnectionRequest() = ControlPacketWrapper(client.connectionRequest)
    override fun getHost() = client.hostname
    override fun getPort() = client.port.toInt()
    override fun usingWebsockets() = client.useWebsockets

    override fun addConnectionCallback(callback: MqttConnectionStateCallback) {
        callbacks += callback
    }

    override fun removeConnectionCallback(callback: MqttConnectionStateCallback) {
        callbacks -= callback
    }

    override fun registerIncomingMessageListener(callback: DataCallback) {
        incomingCb += callback
    }

    override fun unregisterIncomingMessageListener(callback: DataCallback) {
        incomingCb -= callback
    }

    override fun registerOutgoingMessageListener(callback: DataCallback) {
        outgoingCb += callback
    }

    override fun unregisterOutgoingMessageListener(callback: DataCallback) {
        outgoingCb -= callback
    }


    override fun publishAtMostOnce(topic: String, retain: Boolean, payload: Bundle?) {
        val payloadBuffer = payload?.toBufferOrNull()
        client.publishAtMostOnce(topic, retain, payloadBuffer)
    }

    override fun publishAtLeastOnce(
        topic: String,
        retain: Boolean,
        payload: Bundle?,
        persist: Boolean,
        callback: DataCallback
    ) {
        val payloadBuffer = payload?.toBufferOrNull()
        val deferred = client.publishAtLeastOnce(topic, retain, payloadBuffer, persist)
        scope.launch {
            callback.onSuccess(ControlPacketWrapper(deferred.await()))
        }
    }

    override fun publishExactlyOnce(
        topic: String,
        retain: Boolean,
        payload: Bundle?,
        persist: Boolean,
        callbackRecv: DataCallback,
        callbackComp: DataCallback
    ) {
        val payloadBuffer = payload?.toBufferOrNull()
        val deferred = client.publishExactlyOnce(topic, retain, payloadBuffer, persist)
        scope.launch {
            callbackRecv.onSuccess(ControlPacketWrapper(deferred.pubRecv.await()))
            callbackComp.onSuccess(ControlPacketWrapper(deferred.pubComp.await()))
        }
    }

    override fun subscribe(topicFilter: String, persist: Boolean, qos: Byte, callback: DataCallback) {
        val actualQos = when (qos) {
            2.toByte() -> {
                QualityOfService.EXACTLY_ONCE
            }
            1.toByte() -> {
                QualityOfService.AT_LEAST_ONCE
            }
            else -> {
                QualityOfService.AT_MOST_ONCE
            }
        }
        val deferred = client.subscribe(topicFilter, actualQos, persist) { pub ->
            callback.onSuccess(ControlPacketWrapper(pub))
        }
        scope.launch {
            callback.onSuccess(ControlPacketWrapper(deferred.await().second))
        }
    }

    override fun unsubscribe(topicFilter: String, persist: Boolean, callback: DataCallback) {
        val deferred = client.unsubscribe(topicFilter, persist)
        scope.launch {
            callback.onSuccess(ControlPacketWrapper(deferred.await().second))
        }
    }

    override fun observe(topicFilter: String, callback: DataCallback) {
        client.observe(Filter(topicFilter)) { pub ->
            callback.onSuccess(ControlPacketWrapper(pub))
        }
    }

    override fun ping(callback: DataCallback?) {
        scope.launch {
            callback?.onSuccess(ControlPacketWrapper(client.ping().await()))
        }
    }

    override fun pauseReconnects() {
        client.pauseReconnects()
    }

    override fun resumeReconnects() {
        client.resumeReconnects()
    }

    override fun close() {
        scope.launch {
            client.close()
        }
    }

    override fun getInfoString() = client.toString()
}

fun ParcelablePlatformBuffer.toBundle(): Bundle {
    val bundle = Bundle()
    bundle.putParcelable("buf", BufferWrapper(this))
    return bundle
}

fun Bundle.toBufferOrNull(): ParcelablePlatformBuffer? {
    classLoader = BufferWrapper::javaClass.javaClass.classLoader
    return getParcelable<BufferWrapper>("buf")?.buffer
}

