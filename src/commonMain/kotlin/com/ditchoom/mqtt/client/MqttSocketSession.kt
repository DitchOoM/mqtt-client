@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.ParcelablePlatformBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.data.Writer
import com.ditchoom.mqtt.MqttException
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionAcknowledgment
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.*
import com.ditchoom.websocket.WebSocketConnectionOptions
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeMark
import kotlin.time.TimeSource

@ExperimentalTime
class MqttSocketSession private constructor(
    val connectionAcknowledgement: IConnectionAcknowledgment,
    private val timeout: Duration,
    private val writer: Writer<ParcelablePlatformBuffer>,
    private val reader: BufferedControlPacketReader,
    private val socketController: SocketController,
    private val messageSentListener: MutableSharedFlow<ControlPacket>?
) : SuspendCloseable {
    private var isClosed = false
    var lastMessageReceivedTimestamp: TimeMark = TimeSource.Monotonic.markNow()
        private set

    fun isOpen() = !isClosed && reader.isOpen()

    suspend fun write(vararg controlPackets: ControlPacket) {
        writer.write(controlPackets.toBuffer(), timeout)
        if (messageSentListener != null) {
            controlPackets.forEach { messageSentListener.emit(it) }
        }
    }

    suspend fun read() = reader.readControlPacket()

    suspend fun awaitClose(): SocketException = socketController.awaitClose()

    override suspend fun close() {
        isClosed = true
        socketController.close()
    }

    companion object {
        suspend fun openConnection(
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
            socketTimeout: Duration = connectionRequest.keepAliveTimeoutSeconds.toInt().seconds * 1.5,
            socketOptions: SocketOptions? = null,
            messageSentListener: MutableSharedFlow<ControlPacket>? = null,
        ): MqttSocketSession {
            val socket = if (useWebsockets) {
                getWebSocketClient(WebSocketConnectionOptions(hostname, port.toInt(), "mqtt", "/mqtt", socketTimeout))
            } else {
                val s = getClientSocket()
                s.open(hostname = hostname, port = port, timeout = socketTimeout, socketOptions = socketOptions)
                s
            }
            val connect = connectionRequest.toBuffer()
            socket.write(connect, socketTimeout)
            messageSentListener?.emit(connectionRequest)
            val bufferedControlPacketReader =
                BufferedControlPacketReader(connectionRequest.controlPacketFactory, socketTimeout, socket)
            val response = bufferedControlPacketReader.readControlPacket()
            if (response is IConnectionAcknowledgment) {
                return MqttSocketSession(
                    response,
                    socketTimeout,
                    socket,
                    bufferedControlPacketReader,
                    socket,
                    messageSentListener
                )
            }
            throw MqttException(
                "Invalid response received. Expected ConnectionAcknowledgment, instead received $response",
                0x81.toUByte()
            )
        }
    }
}