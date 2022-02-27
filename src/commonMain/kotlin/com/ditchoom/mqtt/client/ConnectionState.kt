package com.ditchoom.mqtt.client

import com.ditchoom.mqtt.base.Parcelable

sealed interface ConnectionState : Parcelable

@Parcelize
data class Connected(val count: Long) : ConnectionState

interface Disconnected : ConnectionState

@Parcelize
data class PermanentlyStopped(val isCancelled: Boolean) : Disconnected

@Parcelize
data class DisconnectedReconnectWithDelay(val delayForReconnectMs: Long) : Disconnected

interface Connecting : ConnectionState

@Parcelize
object Initializing : Connecting

@Parcelize
object ReconnectionPaused : Connecting

@Parcelize
object AttemptingConnection : Connecting

@Parcelize
data class ConnectionAttemptFailed(val errorMessage: String?) : Connecting