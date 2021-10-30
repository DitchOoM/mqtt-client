@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.ditchoom.mqtt.client.net

import com.ditchoom.buffer.SuspendCloseable
import com.ditchoom.mqtt.MqttException
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.IConnectionAcknowledgment
import com.ditchoom.mqtt.controlpacket.IConnectionRequest
import com.ditchoom.socket.ClientSocket
import com.ditchoom.socket.SocketOptions
import com.ditchoom.socket.getClientSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.plus
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MqttSocketSession private constructor(
    val connack: IConnectionAcknowledgment,
    private val socket: ClientSocket,
    private val reader: ControlPacketReader,
    private val writer: ControlPacketWriter,
    private val scope: CoroutineScope,
): SuspendCloseable {

    suspend fun write(vararg controlPackets: ControlPacket) {
        writer.write(controlPackets)
    }

    suspend fun read() = reader.read()

    override suspend fun close() {
        reader.close()
        writer.close()
        socket.close()
        scope.cancel()
    }

    companion object {
        suspend fun openConnection(
            scope: CoroutineScope,
            connectionRequest: IConnectionRequest,
            port: UShort,
            hostname: String = "localhost",
            socketTimeout: Duration = Duration.seconds(connectionRequest.keepAliveTimeoutSeconds.toDouble() * 1.5),
            socketOptions: SocketOptions? = null,
        ): MqttSocketSession {
            val childScope = scope + Job()
            val socket = getClientSocket()
            socket.open(hostname = hostname, port = port, timeout = socketTimeout, socketOptions = socketOptions)
            val writer = ControlPacketWriter.build(childScope, socket, connectionRequest)
            writer.write(connectionRequest)
            val reader = ControlPacketReader.build(childScope, socket, connectionRequest)
            val response = reader.read()
            if (response is IConnectionAcknowledgment) {
                return MqttSocketSession(response, socket, reader, writer, childScope)
            }
            throw MqttException(
                "Invalid response received. Expected ConnectionAcknowledgment, instead received $response",
                0x81.toUByte()
            )
        }
    }
}