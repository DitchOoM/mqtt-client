package com.ditchoom.mqtt.client.net

import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.ClientSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ControlPacketReader private constructor(private val bufferedControlPacketReader: BufferedControlPacketReader): SuspendCloseable {
    private val incomingControlPacketChannel = Channel<ControlPacket>()
    suspend fun read() = incomingControlPacketChannel.receive()

    private suspend fun listenForControlPackets()  {
        bufferedControlPacketReader.readControlPackets().collect { controlPacket ->
            incomingControlPacketChannel.send(controlPacket)
        }
        close()
    }

    override suspend fun close() {
        bufferedControlPacketReader.close()
        incomingControlPacketChannel.cancel()
        incomingControlPacketChannel.close()
    }

    companion object {
        suspend fun build(scope: CoroutineScope, socket: ClientSocket, connectionRequest: IConnectionRequest): ControlPacketReader {
            val mqttTimeout = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toInt()) * 1.5
            val bufferedControlPacketReader =
                BufferedControlPacketReader.build(scope, socket, mqttTimeout, connectionRequest.controlPacketFactory)
            val reader = ControlPacketReader(bufferedControlPacketReader)
            scope.launch { reader.listenForControlPackets() }
            return reader
        }
    }
}