/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.nio.ByteBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBlockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport, mapId2PartitionId: Map[Long, Int]) extends BlockStoreClient with Logging {

  private def getBlockFromDpu(shuffleId: Int, mapId: Long, reducePartitionId: Int,
                              blockId: String, listener: BlockFetchingListener, execId: String): Unit = {
    val ucxTransport: UcxShuffleTransport = transport

    logDebug(s"LEO UcxShuffleBlockResolver getBlockData from DPU shuffleId $shuffleId mapId $mapId reduceId $reducePartitionId partId ${mapId2PartitionId(mapId)}")

    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    val callbacks = Array.ofDim[OperationCallback](1)
    var fetchDone = false

    callbacks(0) = (result: OperationResult) => {
        val memBlock = result.getData
        val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
        logDebug(s"LEO Fetched block from DPU ${memBlock.size.toInt}")
        var remoteResultBuffer = ByteBuffer.allocate(memBlock.size.toInt);
        remoteResultBuffer.put(buffer)
        remoteResultBuffer.flip()
        memBlock.close()
        fetchDone = true
        listener.onBlockFetchSuccess(blockId, new NioManagedBuffer(remoteResultBuffer) {
          override def release: ManagedBuffer = {
            this
          }
        })
    }
    val req = ucxTransport.fetchBlock(execId.toLong, shuffleId, mapId, reducePartitionId, resultBufferAllocator, callbacks)

    while (!fetchDone) {
      ucxTransport.progress()
    }
  }

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    logDebug(s"LEO entered fetchBlocks ($host:$port) execId $execId")
    if (blockIds.length > transport.ucxShuffleConf.maxBlocksPerRequest) {
      val (b1, b2) = blockIds.splitAt(blockIds.length / 2)
      fetchBlocks(host, port, execId, b1, listener, downloadFileManager)
      fetchBlocks(host, port, execId, b2, listener, downloadFileManager)
      return
    }


    val ucxBlockIds = Array.ofDim[UcxShuffleBlockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      // val buffer = getBlockFromDpu(blockId.shuffleId, blockId.mapId, blockId.reduceId)
      getBlockFromDpu(blockId.shuffleId, blockId.mapId, blockId.reduceId, blockIds(i), listener, execId)
      // listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
      //     override def release: ManagedBuffer = {
      //       memBlock.close()
      //       this
      //     }
      // })

      // ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, mapId2PartitionId(blockId.mapId), blockId.reduceId)
      // callbacks(i) = (result: OperationResult) => {
      //   val memBlock = result.getData
      //   val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
      //   listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
      //     override def release: ManagedBuffer = {
      //       memBlock.close()
      //       this
      //     }
      //   })
      // }

    }

    // val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    // transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds, resultBufferAllocator, callbacks)
    // transport.progress()
  }

  override def close(): Unit = {

  }
}
