@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client

import com.ditchoom.buffer.PlatformBuffer
import com.ditchoom.buffer.ReadBuffer
import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.data.Writer
import com.ditchoom.mqtt.MqttException
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionAcknowledgment
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.SocketOptions
import com.ditchoom.socket.getClientSocket
import com.ditchoom.socket.getWebSocketClient
import com.ditchoom.websocket.WebSocketConnectionOptions
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeMark
import kotlin.time.TimeSource

@ExperimentalTime
class MqttSocketSession private constructor(
    val connack: IConnectionAcknowledgment,
    private val timeout: Duration,
    private val writer: Writer<PlatformBuffer>,
    private val reader: BufferedControlPacketReader,
    private val closable: SuspendCloseable,
): SuspendCloseable {
    var lastMessageReceivedTimestamp :TimeMark = TimeSource.Monotonic.markNow()
        private set

    fun isOpen() = reader.isOpen()

    suspend fun write(vararg controlPackets: ControlPacket) {
        writer.write(controlPackets.toBuffer(), timeout)
    }

    suspend fun read() = reader.readControlPacket()

    override suspend fun close() {
        closable.close()
    }

    companion object {
        suspend fun openConnection(
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            useWebsockets: Boolean = false,
            socketTimeout: Duration = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toDouble() * 1.5),
            socketOptions: SocketOptions? = null,
        ): MqttSocketSession {
            val socket =  if (useWebsockets) {
                getWebSocketClient(WebSocketConnectionOptions(hostname, port.toInt(), "mqtt", "/mqtt", socketTimeout))
            } else {
                val s = getClientSocket()
                s.open(hostname = hostname, port = port, timeout = socketTimeout, socketOptions = socketOptions)
                s
            }
            val connect = connectionRequest.toBuffer()
            socket.write(connect, socketTimeout)
            val bufferedControlPacketReader =
                BufferedControlPacketReader(connectionRequest.controlPacketFactory, socketTimeout, socket)
            val response = bufferedControlPacketReader.readControlPacket()
            if (response is IConnectionAcknowledgment) {
                return MqttSocketSession(response, socketTimeout, socket, bufferedControlPacketReader, socket)
            }
            throw MqttException(
                "Invalid response received. Expected ConnectionAcknowledgment, instead received $response",
                0x81.toUByte()
            )
        }
    }
}