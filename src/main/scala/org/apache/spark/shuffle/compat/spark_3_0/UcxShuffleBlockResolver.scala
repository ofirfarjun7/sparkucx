/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer

import org.apache.spark.{TaskContext, SparkEnv}
import org.apache.spark.storage._
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleTransport, CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}


/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  // TODO: Init UCX worker here; Is this a singleton?
  extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  val shuffleManager: CommonUcxShuffleManager = ucxShuffleManager


  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    // In Spark-3.0 MapId is long and unique among all jobs in spark. We need to use partitionId as offset
    // in metadata buffer
    val partitionId = TaskContext.getPartitionId()
    val dataFile = getDataFile(shuffleId, mapId)
    if (!dataFile.exists() || dataFile.length() == 0) {
      return
    }
    writeIndexFileAndCommitCommon(shuffleId, partitionId, lengths, new RandomAccessFile(dataFile, "r"))
  }

  private def getBlockFromDpu(shuffleId: Int, mapId: Long, reducePartitionId: Int): ByteBuffer = {
    val ucxTransport: UcxShuffleTransport = shuffleManager.ucxTransport

    logDebug("LEO UcxShuffleBlockResolver getBlockData from DPU")

    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    val callbacks = Array.ofDim[OperationCallback](1)
    var fetchDone = false
    var remoteResultBuffer = ByteBuffer.allocate(0);
    callbacks(0) = (result: OperationResult) => {
        val memBlock = result.getData
        val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
        logDebug(s"LEO Fetched block from DPU ${memBlock.size.toInt}")
        remoteResultBuffer = ByteBuffer.allocate(memBlock.size.toInt);
        remoteResultBuffer.put(buffer)
        remoteResultBuffer.flip()
        memBlock.close()
        fetchDone = true
    }

    //TODO - adapt code to new fetch block async method
    // val req = ucxTransport.fetchBlock(1, shuffleId, mapId, reducePartitionId, resultBufferAllocator, callbacks)

    // while (!fetchDone) {
    //   ucxTransport.progress()
    // }
    
    remoteResultBuffer
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    // super.getBlockData(blockId, dirs)

    val ucxTransport: UcxShuffleTransport = shuffleManager.ucxTransport

    logDebug("LEO UcxShuffleBlockResolver getBlockData")
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
     }
    
    var dpuEnabled = SparkEnv.get.conf.getBoolean("spark.dpuTest.enabled", true)
    if (dpuEnabled) {
      logDebug(s"LEO UcxShuffleBlockResolver - Reading $shuffleId mapId $mapId reduceId $startReduceId from the DPU")
      var resultBuffer = getBlockFromDpu(shuffleId, mapId, startReduceId)
      new NioManagedBuffer(resultBuffer)
    } else {
      var length = ucxTransport.getNvkvWrapper.getPartitonLength(shuffleId, mapId, startReduceId).toInt
      var offset = ucxTransport.getNvkvWrapper.getPartitonOffset(shuffleId, mapId, startReduceId)
      logDebug(s"LEO UcxShuffleBlockResolver - Reading shuffleId $shuffleId mapId $mapId reduceId $startReduceId at offset $offset with length $length from nvkv")
      var resultBuffer = ucxTransport.getNvkvWrapper.read(length, offset)
      new NioManagedBuffer(resultBuffer)
    }
  }
}
