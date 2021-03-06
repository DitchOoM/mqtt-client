package com.ditchoom.mqtt.client

import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.topic.Filter
import kotlinx.coroutines.Deferred

interface IMqttClient : SuspendCloseable {
    fun publishAtMostOnce(topic: CharSequence, retain: Boolean = false): Deferred<Unit>
    fun publishAtMostOnce(topic: CharSequence, retain: Boolean = false, payload: String? = null): Deferred<Unit>
    fun publishAtMostOnce(
        topic: CharSequence,
        retain: Boolean = false,
        payload: ParcelablePlatformBuffer? = null
    ): Deferred<Unit>

    fun publishAtLeastOnce(
        topic: CharSequence,
        retain: Boolean = false,
        persist: Boolean = true
    ): Deferred<IPublishAcknowledgment>

    fun publishAtLeastOnce(
        topic: CharSequence,
        retain: Boolean = false,
        payload: String? = null,
        persist: Boolean = true
    ): Deferred<IPublishAcknowledgment>

    fun publishAtLeastOnce(
        topic: CharSequence,
        retain: Boolean = false,
        payload: ParcelablePlatformBuffer? = null,
        persist: Boolean = true
    ): Deferred<IPublishAcknowledgment>

    fun publishExactlyOnce(
        topic: CharSequence,
        retain: Boolean = false,
        persist: Boolean = true
    ): DeferredPublishExactlyOnceResponse

    fun publishExactlyOnce(
        topic: CharSequence,
        retain: Boolean = false,
        payload: String? = null,
        persist: Boolean = true
    ): DeferredPublishExactlyOnceResponse

    fun publishExactlyOnce(
        topic: CharSequence,
        retain: Boolean = false,
        payload: ParcelablePlatformBuffer? = null,
        persist: Boolean = true
    ): DeferredPublishExactlyOnceResponse

    fun subscribe(
        topicFilter: CharSequence,
        maximumQos: QualityOfService = QualityOfService.AT_LEAST_ONCE,
        noLocal: Boolean = false,
        retainAsPublished: Boolean = false,
        retainHandling: ISubscription.RetainHandling = ISubscription.RetainHandling.SEND_RETAINED_MESSAGES_AT_TIME_OF_SUBSCRIBE,
        serverReference: CharSequence? = null,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList(),
        persist: Boolean = true,
        callback: ((IPublishMessage) -> Unit)? = null
    ): Deferred<Pair<ISubscribeRequest, ISubscribeAcknowledgement>>

    fun unsubscribe(
        topic: String,
        persist: Boolean = true,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ): Deferred<Pair<IUnsubscribeRequest, IUnsubscribeAcknowledgment>>

    fun unsubscribe(
        topics: Set<String>,
        persist: Boolean = true,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ): Deferred<Pair<IUnsubscribeRequest, IUnsubscribeAcknowledgment>>

    fun observe(topicFilter: Filter, callback: (IPublishMessage) -> Unit)
    fun ping(): Deferred<IPingResponse>
}