package org.apache.spark.shuffle.ucx

/*
    * Licensed to the Apache Software Foundation (ASF) under one or more
    * contributor license agreements.  See the NOTICE file distributed with
    * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.internal.Logging
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel
import java.util.Optional
import scala.util.Random
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.shuffle.api.WritableByteChannelWrapper
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.util.Utils
import org.apache.spark.shuffle.ucx
import org.apache.spark.shuffle.utils.{CommonUtils, UnsafeUtils}


/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

// Copied from:
// https://raw.githubusercontent.com/apache/spark/v3.0.3/core/src/main/java/org/apache/spark/shuffle/sort/io/LocalDiskShuffleMapOutputWriter.java


/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


class NvkvShuffleMapOutputWriter(private val shuffleId: Int, 
                                 private val mapId: Long, 
                                 numPartitions: Int, 
                                 val ucxTransport: UcxShuffleTransport,
                                 private val blockResolver: IndexShuffleBlockResolver, 
                                 sparkConf: SparkConf) extends ShuffleMapOutputWriter with Logging {
  logDebug(s"NvkvShuffleMapOutputWriter for shuffleId $shuffleId mapId $mapId numPartitions $numPartitions")

  final private var partitionLengths: Array[Long] = new Array[Long](numPartitions)
  final private var partitionsPadding: Array[Long] = new Array[Long](numPartitions)
  private var totalPartitionsPadding: Long = 0
  final private var bufferSize = 4096
  private var lastPartitionId = -1
  private var currChannelPosition = 0L
  private var bytesWrittenToMergedFile = 0L
  private var outputBufferedFileStream: BufferedOutputStream = null
  private var nvkvWrapper: NvkvWrapper = ucxTransport.getNvkvWrapper
  private var executerId = SparkEnv.get.blockManager.blockManagerId.executorId.toLong

  private def getBlockOffset = {
    //TODO - add executer partition offset
    val numShuffles = 1
    val numOfMappers = SparkEnv.get.conf.getInt("spark.groupByTest.numMappers", 1)
    val shuffleBlockSize = nvkvWrapper.getPartitionSize / numShuffles
    val mapBlockSize = shuffleBlockSize / numOfMappers
    val reminder = mapBlockSize % nvkvWrapper.getAlignment;
    val alignedMapBlockSize = if (reminder != 0) {mapBlockSize - (reminder)} else mapBlockSize
    (shuffleId * shuffleBlockSize) + (mapId * alignedMapBlockSize)
  }

  @throws[IOException]
  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    logDebug(s"NvkvShuffleMapOutputWriter getPartitionWriter for reducePartition $reducePartitionId")
    if (reducePartitionId <= lastPartitionId) throw new IllegalArgumentException("Partitions should be requested in increasing order.")
    lastPartitionId = reducePartitionId
    currChannelPosition = getBlockOffset + bytesWrittenToMergedFile + totalPartitionsPadding
    logDebug(s"NvkvShuffleMapOutputWriter reducePartition offset $currChannelPosition")
    new NvkvShufflePartitionWriter(reducePartitionId)
  }

  @throws[IOException]
  override def commitAllPartitions: Array[Long] = {
    logDebug("NvkvShuffleMapOutputWriter commitAllPartitions")
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.

    var blockOffset = getBlockOffset;

    //TODO - move to shuffleTransport    
    var packMapperData: ByteBuffer = ByteBuffer.allocateDirect(UnsafeUtils.INT_SIZE + // numOfMappers
                                                               UnsafeUtils.INT_SIZE + // numOfReducePartitions
                                                               UnsafeUtils.INT_SIZE + // mapperId
                                                               2*UnsafeUtils.LONG_SIZE*partitionLengths.size // offset + length for each reducePartition
                                                              ).order(ByteOrder.LITTLE_ENDIAN)
    packMapperData.putInt(1)
    packMapperData.putInt(partitionLengths.size)
    packMapperData.putInt(mapId.toInt)

    partitionLengths.zip(0 until partitionLengths.size).foreach{ 
        case (partitionLength, reduceId) => {
            logDebug(s"shuffleId $shuffleId mapId $mapId reducerId $reduceId offset $blockOffset size $partitionLength")
            logDebug(s"Reduce partition stats: offset ${nvkvWrapper.getPartitonOffset(shuffleId, mapId, reduceId)} length ${nvkvWrapper.getPartitonLength(shuffleId, mapId, reduceId)} padding ${partitionsPadding(reduceId)}")
            packMapperData.putLong(nvkvWrapper.getPartitonOffset(shuffleId, mapId, reduceId))
            packMapperData.putLong(nvkvWrapper.getPartitonLength(shuffleId, mapId, reduceId))
            blockOffset += partitionLength
        }
    }

    packMapperData.rewind()
    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    var commitBlock = false
    logDebug(s"Sending map partition information for mapId $mapId to DPU")
    ucxTransport.commitBlock(executerId, resultBufferAllocator,
      packMapperData, () => {commitBlock = true})
    // TODO - need to find a better way yo commit blocks locations to DPU
    // Idealy we will want to send all the locations at the end of the write stage
    // and not during the write stage with samllest number of AM messages (1) as possible.
    // The problem is how we can tell that we reached the last map partition.
    CommonUtils.safePolling(() => {ucxTransport.progress()}, () => {!commitBlock})
    partitionLengths
  }

  @throws[IOException]
  override def abort(error: Throwable): Unit = {
    cleanUp()
  }

  @throws[IOException]
  private def cleanUp(): Unit = {
    logDebug("NvkvShuffleMapOutputWriter cleanUp")
  }

  @throws[IOException]
  private def initStream(): Unit = {
    logDebug(s"NvkvShuffleMapOutputWriter initStream bufferSize $bufferSize")
  }

  private class NvkvShufflePartitionWriter (private val partitionId: Int) extends ShufflePartitionWriter {
    logDebug("NvkvShufflePartitionWriter ctor " + partitionId + " shuffleId " + shuffleId + " mapId " + mapId)
    private var partStream: PartitionWriterStream = null

    @throws[IOException]
    override def openStream: OutputStream = {
      logDebug("NvkvShufflePartitionWriter openStream " + partitionId)
      if (partStream == null) {
        initStream()
        partStream = new PartitionWriterStream(partitionId)
      }
      partStream
    }

    @throws[IOException]
    override def openChannelWrapper: Optional[WritableByteChannelWrapper] = {
      Optional.empty()
    }

    override def getNumBytesWritten: Long = {
      logDebug("NvkvShufflePartitionWriter getNumBytesWritten " + partitionId)
      if (partStream != null) partStream.getCount
      else {
        // Assume an empty partition if stream and channel are never created
        0
      }
    }
  }

  private class PartitionWriterStream private[ucx](private val partitionId: Int) extends OutputStream {
    logDebug("PartitionWriterStream ctor " + partitionId)
    private var count = 0
    private var isClosed = false

    def getCount: Long = count

    @throws[IOException]
    override def write(b: Int): Unit = {
      logDebug("PartitionWriterStream write1 " + b)
      verifyNotClosed()
      count += 1
    }

    @throws[IOException]
    override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
      val offset = currChannelPosition + count
      logDebug(s"PartitionWriterStream write2 $shuffleId,$mapId,$partitionId buf $buf pos $pos length $length offset $offset")
      nvkvWrapper.write(shuffleId, mapId, partitionId, buf, length, offset)
      verifyNotClosed()
      count += length
    }

    override def close(): Unit = {
      var padding: Int = nvkvWrapper.writeRemaining(currChannelPosition+count)
      nvkvWrapper.commitPartition(currChannelPosition, count, shuffleId, mapId, partitionId)
      isClosed = true
      partitionLengths(partitionId) = count
      totalPartitionsPadding += padding
      partitionsPadding(partitionId) = padding
      bytesWrittenToMergedFile += count
      logDebug(s"PartitionWriterStream close currChannelPosition ${currChannelPosition}")
      logDebug(s"PartitionWriterStream close $shuffleId,$mapId,$partitionId count $count padding $padding bytesWrittenToMergedFile $bytesWrittenToMergedFile")
    }

    private def verifyNotClosed(): Unit = {
      if (isClosed) throw new IllegalStateException("Attempting to write to a closed block output stream.")
    }
  }
}
