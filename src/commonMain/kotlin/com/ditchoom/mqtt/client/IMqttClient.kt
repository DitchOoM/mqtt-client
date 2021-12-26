package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.mqtt.controlpacket.*
import com.ditchoom.mqtt.topic.Filter
import kotlinx.coroutines.Deferred

interface IMqttClient: SuspendCloseable {
    fun publishAtMostOnce(topic: CharSequence): Deferred<Unit>
    fun publishAtMostOnce(topic: CharSequence, payload: String? = null): Deferred<Unit>
    fun publishAtMostOnce(topic: CharSequence, payload: PlatformBuffer? = null): Deferred<Unit>
    fun publishAtLeastOnce(topic: CharSequence): Deferred<IPublishAcknowledgment>
    fun publishAtLeastOnce(topic: CharSequence, payload: String? = null): Deferred<IPublishAcknowledgment>
    fun publishAtLeastOnce(topic: CharSequence, payload: PlatformBuffer? = null): Deferred<IPublishAcknowledgment>
    fun publishExactlyOnce(topic: CharSequence): Deferred<Unit>
    fun publishExactlyOnce(topic: CharSequence, payload: String? = null): Deferred<Unit>
    fun publishExactlyOnce(topic: CharSequence, payload: PlatformBuffer? = null): Deferred<Unit>
    fun subscribe(
        topicFilter: CharSequence,
        maximumQos: QualityOfService = QualityOfService.AT_LEAST_ONCE,
        noLocal: Boolean = false,
        retainAsPublished: Boolean = false,
        retainHandling: ISubscription.RetainHandling = ISubscription.RetainHandling.SEND_RETAINED_MESSAGES_AT_TIME_OF_SUBSCRIBE,
        serverReference: CharSequence? = null,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList(),
        callback: ((IPublishMessage) -> Unit)? = null
    ): Deferred<Pair<ISubscribeRequest, ISubscribeAcknowledgement>>

    fun unsubscribe(
        topic: String,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ): Deferred<Pair<IUnsubscribeRequest, IUnsubscribeAcknowledgment>>

    fun unsubscribe(
        topics: Set<String>,
        userProperty: List<Pair<CharSequence, CharSequence>> = emptyList()
    ): Deferred<Pair<IUnsubscribeRequest, IUnsubscribeAcknowledgment>>

    fun observe(topicFilter: Filter, callback: (IPublishMessage) -> Unit)
    fun ping(): Deferred<IPingResponse>
}