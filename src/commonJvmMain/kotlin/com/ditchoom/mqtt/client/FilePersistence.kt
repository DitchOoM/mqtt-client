package com.ditchoom.mqtt.client

import com.ditchoom.buffer.JvmBuffer
import com.ditchoom.mqtt.controlpacket.ControlPacket
import com.ditchoom.mqtt.controlpacket.ControlPacketFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import java.io.File
import java.nio.channels.FileChannel

@ExperimentalUnsignedTypes
class FilePersistence(private val factory: ControlPacketFactory, clientId:String, host:String, port:Int, parentDirectory: File): Persistence {
    val localDirectory = File(File(parentDirectory, "$host:$port"), clientId).also { it.mkdirs() }
    val controlPacketDirectory = File(localDirectory, "control-packets").also { it.mkdirs() }

    override suspend fun getAllPendingIds(): Set<Int> {
        val set = HashSet<Int>()
        controlPacketDirectory.iterateFilesFast { file ->
            file.name.toIntOrNull()?.let { num -> set += num }
        }
        return set
    }

    override suspend fun nextPacketIdentifier(persist: Boolean): Int {
        var packetId = 1
        var file :File
        withContext(Dispatchers.IO) {
            file = File(controlPacketDirectory, "$packetId")
            while (file.exists()) {
                packetId++
                file = File(controlPacketDirectory, "$packetId")
            }
            file.createNewFile()
        }
        return packetId
    }

    override suspend fun save(packetIdentifier: Int, data: ControlPacket) = withContext(Dispatchers.IO) {
        File(controlPacketDirectory, "$packetIdentifier")
            .inputStream().channel.use { fileChannel ->
                val mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, data.packetSize().toLong())
                val buffer = JvmBuffer(mappedByteBuffer)
                data.serialize(buffer)
            }
    }

    override suspend fun delete(id: Int): Unit = withContext(Dispatchers.IO) {
        File(controlPacketDirectory, "$id").delete()
    }

    override suspend fun queuedControlPackets() = flow<Pair<Int, ControlPacket>> {
        controlPacketDirectory.iterateFilesFast { file ->
            val packetIdentifier = file.name.toIntOrNull()
            if (packetIdentifier != null) {
                val packet = file.inputStream().channel.use { fileChannel ->
                    val mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())
                    val buffer = JvmBuffer(mappedByteBuffer)
                    factory.from(buffer)
                }
                emit(Pair(packetIdentifier, packet))
            }
        }
    }.flowOn(Dispatchers.IO)
}