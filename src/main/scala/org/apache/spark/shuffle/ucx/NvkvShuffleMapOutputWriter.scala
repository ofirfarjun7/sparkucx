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
//  package org.apache.spark.shuffle.sort.io;

import org.apache.spark.internal.Logging
import org.apache.log4j.Logger
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
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.shuffle.api.WritableByteChannelWrapper
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.util.Utils
import org.apache.spark.shuffle.ucx


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

//  package org.apache.spark.shuffle.sort.io;


object NvkvShuffleMapOutputWriter {
  private val log = Logger.getLogger("LEO")
}

object NvkvRandomDevice {
  private val rand = new scala.util.Random
  def getDeviceId(numOfDevices: Int): Int = {
    rand.nextInt(numOfDevices)
  }
}

class NvkvShuffleMapOutputWriter(private val shuffleId: Int, 
                                 private val mapId: Long, 
                                 numPartitions: Int, 
                                 val ucxTransport: UcxShuffleTransport,
                                 private val blockResolver: IndexShuffleBlockResolver, 
                                 sparkConf: SparkConf) extends ShuffleMapOutputWriter {
  NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter2 ctor shuffleId " + shuffleId + " mapId " + mapId + " numPartitions " + numPartitions)

  final private var partitionLengths: Array[Long] = new Array[Long](numPartitions)
  final private var partitionsPadding: Array[Long] = new Array[Long](numPartitions)
  private var totalPartitionsPadding: Long = 0
  final private var bufferSize = 4096
  private var lastPartitionId = -1
  private var currChannelPosition = 0L
  private var dsIdx: Int = 0
  private var bytesWrittenToMergedFile = 0L
  private var outputBufferedFileStream: BufferedOutputStream = null
  private var nvkvHandler: NvkvHandler = ucxTransport.getNvkvHandler
  private var executerId = SparkEnv.get.blockManager.blockManagerId.executorId.toLong

  private def getBlockOffset = {
    //TODO - add executer partition offset
    val numShuffles = 1
    val numOfMappers = SparkEnv.get.conf.getInt("spark.groupByTest.numMappers", 1)
    val shuffleBlockSize = nvkvHandler.getPartitionSize / numShuffles
    val mapBlockSize = shuffleBlockSize / numOfMappers
    val r = mapBlockSize % 512;
    val alignedMapBlockSize = if (r != 0) {mapBlockSize - (r)} else mapBlockSize
    (shuffleId * shuffleBlockSize) + (mapId * alignedMapBlockSize)
  }

  @throws[IOException]
  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter getPartitionWriter " + reducePartitionId)
    if (reducePartitionId <= lastPartitionId) throw new IllegalArgumentException("Partitions should be requested in increasing order.")
    lastPartitionId = reducePartitionId
    currChannelPosition = getBlockOffset + bytesWrittenToMergedFile + totalPartitionsPadding
    // val rand = new scala.util.Random
    // dsIdx = rand.nextInt(nvkvHandler.getNumOfStorage)
    dsIdx = NvkvRandomDevice.getDeviceId(nvkvHandler.getNumOfDevices)
    NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter currChannelPosition " + currChannelPosition)
    new NvkvShufflePartitionWriter(reducePartitionId)
  }

  @throws[IOException]
  override def commitAllPartitions: Array[Long] = {
    NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter commitAllPartitions")
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.

    var blockOffset = getBlockOffset;

    //TODO - move to shuffleTransport    
    var packMapperData: ByteBuffer = ByteBuffer.allocateDirect(8 + 8 + 4 + 2*8*partitionLengths.size).order(ByteOrder.nativeOrder())
    packMapperData.putInt(1)
    packMapperData.putInt(partitionLengths.size)
    packMapperData.putInt(mapId.toInt)

    partitionLengths.zip(0 until partitionLengths.size).foreach{ 
        case (partitionLength, reduceId) => {
            NvkvShuffleMapOutputWriter.log.debug(s"shuffleId $shuffleId mapId $mapId reducerId $reduceId offset $blockOffset size $partitionLength")
            NvkvShuffleMapOutputWriter.log.debug(s"padding ${partitionsPadding(reduceId)} offset ${nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId)} length ${nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId)}")
            NvkvShuffleMapOutputWriter.log.debug(s"Send DPU AM: shuffleId $shuffleId mapId $mapId reducerId $reduceId offset ${nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId)} size ${nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId)}")
            packMapperData.putLong(nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId))
            packMapperData.putLong(nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId))
            blockOffset += partitionLength
        }
    }

    packMapperData.rewind
    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    ucxTransport.commitBlock(executerId, resultBufferAllocator, packMapperData)
    NvkvShuffleMapOutputWriter.log.debug("Writing shuffle index file for mapId " + mapId + " with lengths " + partitionLengths(0) + " " + partitionLengths(1))
    partitionLengths
  }

  @throws[IOException]
  override def abort(error: Throwable): Unit = {
    cleanUp()
  }

  @throws[IOException]
  private def cleanUp(): Unit = {
    NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter cleanUp")
  }

  @throws[IOException]
  private def initStream(): Unit = {
    NvkvShuffleMapOutputWriter.log.debug(s"NvkvShuffleMapOutputWriter initStream bufferSize $bufferSize")
  }

  @throws[IOException]
  private def initChannel(): Unit = {
    NvkvShuffleMapOutputWriter.log.debug("NvkvShuffleMapOutputWriter initChannel")
    // This file needs to opened in append mode in order to work around a Linux kernel bug that
    // affects transferTo; see SPARK-3948 for more details.
  }

  private class NvkvShufflePartitionWriter (private val partitionId: Int) extends ShufflePartitionWriter {
    NvkvShuffleMapOutputWriter.log.debug("NvkvShufflePartitionWriter ctor " + partitionId + " shuffleId " + shuffleId + " mapId " + mapId)
    private var partStream: PartitionWriterStream = null
    private var partChannel: PartitionWriterChannel = null

    @throws[IOException]
    override def openStream: OutputStream = {
      NvkvShuffleMapOutputWriter.log.debug("NvkvShufflePartitionWriter openStream " + partitionId)
      if (partStream == null) {
        initStream()
        partStream = new PartitionWriterStream(partitionId)
      }
      partStream
    }

    @throws[IOException]
    override def openChannelWrapper: Optional[WritableByteChannelWrapper] = {
      NvkvShuffleMapOutputWriter.log.debug("NvkvShufflePartitionWriter openChannelWrapper " + partitionId)
      if (partChannel == null) {
        if (partStream != null) throw new IllegalStateException("Requested an output stream for a previous write but" + " now an output channel has been requested. Should not be using both channels" + " and streams to write.")
        initChannel()
        partChannel = new PartitionWriterChannel(partitionId)
      }
      Optional.of(partChannel)
    }

    override def getNumBytesWritten: Long = {
      NvkvShuffleMapOutputWriter.log.debug("NvkvShufflePartitionWriter getNumBytesWritten " + partitionId)
      if (partChannel != null) try partChannel.getCount
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
      else if (partStream != null) partStream.getCount
      else {
        // Assume an empty partition if stream and channel are never created
        0
      }
    }
  }

  private class PartitionWriterStream private[ucx](private val partitionId: Int) extends OutputStream {
    NvkvShuffleMapOutputWriter.log.debug("PartitionWriterStream ctor " + partitionId)
    private var count = 0
    private var isClosed = false

    def getCount: Long = count

    @throws[IOException]
    override def write(b: Int): Unit = {
      NvkvShuffleMapOutputWriter.log.debug("PartitionWriterStream write1 " + b)
      verifyNotClosed()
      count += 1
    }

    @throws[IOException]
    override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
      val offset = currChannelPosition + count
      NvkvShuffleMapOutputWriter.log.debug(s"PartitionWriterStream write2 $shuffleId,$mapId,$partitionId buf $buf pos $pos length $length offset $offset")
      nvkvHandler.write(dsIdx, shuffleId, mapId, partitionId, buf, length, offset)
      verifyNotClosed()
      count += length
    }

    override def close(): Unit = {
      var padding: Int = nvkvHandler.writeRemaining(dsIdx, currChannelPosition+count)
      nvkvHandler.commitPartition(dsIdx, currChannelPosition, count, shuffleId, mapId, partitionId)
      isClosed = true
      partitionLengths(partitionId) = count
      totalPartitionsPadding += padding
      partitionsPadding(partitionId) = padding
      bytesWrittenToMergedFile += count
      NvkvShuffleMapOutputWriter.log.debug(s"PartitionWriterStream close1 currChannelPosition ${currChannelPosition}")
      NvkvShuffleMapOutputWriter.log.debug(s"PartitionWriterStream close1 $shuffleId,$mapId,$partitionId count $count padding $padding bytesWrittenToMergedFile $bytesWrittenToMergedFile")
    }

    private def verifyNotClosed(): Unit = {
      if (isClosed) throw new IllegalStateException("Attempting to write to a closed block output stream.")
    }
  }

  private class PartitionWriterChannel private[ucx](private val partitionId: Int) extends WritableByteChannelWrapper {
    NvkvShuffleMapOutputWriter.log.debug("PartitionWriterChannel ctor " + partitionId)

    @throws[IOException]
    def getCount: Long = {
      NvkvShuffleMapOutputWriter.log.debug("PartitionWriterChannel getCount " + partitionId)
      0
    }

    override def channel: WritableByteChannel = {
      NvkvShuffleMapOutputWriter.log.debug("PartitionWriterChannel channel " + partitionId)
      null
    }

    @throws[IOException]
    override def close(): Unit = {
      NvkvShuffleMapOutputWriter.log.debug("PartitionWriterStream close2")
      partitionLengths(partitionId) = getCount
      bytesWrittenToMergedFile += partitionLengths(partitionId)
    }
  }
}
