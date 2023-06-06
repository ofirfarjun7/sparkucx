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
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.shuffle.api.WritableByteChannelWrapper
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.util.Utils
import org.apache.spark.shuffle.ucx

import org.apache.spark.shuffle.compat.spark_3_0.UcxShuffleBlockResolver


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

class NvkvShuffleMapOutputWriter(private val shuffleId: Int, 
                                 private val mapId: Long, 
                                 numPartitions: Int, 
                                 val ucxTransport: UcxShuffleTransport,
                                 private val blockResolver: IndexShuffleBlockResolver, 
                                 sparkConf: SparkConf) extends ShuffleMapOutputWriter {
  NvkvShuffleMapOutputWriter.log.info("NvkvShuffleMapOutputWriter2 ctor shuffleId " + shuffleId + " mapId " + mapId + " numPartitions " + numPartitions)

  final private var partitionLengths: Array[Long] = new Array[Long](numPartitions)
  final private var partitionsPadding: Array[Long] = new Array[Long](numPartitions)
  private var totalPartitionsPadding: Long = 0
  final private var bufferSize = 4096
  private var lastPartitionId = -1
  private var currChannelPosition = 0L
  private var bytesWrittenToMergedFile = 0L
  final private var outputFile: File = blockResolver.getDataFile(shuffleId, mapId)
  private var outputTempFile: File = null
  private var outputFileStream: FileOutputStream = null
  private var outputFileChannel: FileChannel = null
  private var outputBufferedFileStream: BufferedOutputStream = null
  private var nvkvHandler: NvkvHandler = ucxTransport.getNvkvHandler

  private def getPartitionOffset(recudeId: Int): Long = {
    //TODO - read from conf
    //TODO - add executer partition offset
    val numShuffles = 1
    val numOfMappers = 8
    val numOfReducers = 4
    val shuffleBlockSize = nvkvHandler.getPartitionSize / numShuffles
    val mapBlockSize = shuffleBlockSize / numOfMappers
    val reducePartitionSize = mapBlockSize / numOfReducers
    var length: Long = (shuffleId * shuffleBlockSize) + (mapId * mapBlockSize) + (recudeId * reducePartitionSize)
    (length - (length % 512))
  }

  @throws[IOException]
  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    NvkvShuffleMapOutputWriter.log.info("NvkvShuffleMapOutputWriter getPartitionWriter " + reducePartitionId)
    if (reducePartitionId <= lastPartitionId) throw new IllegalArgumentException("Partitions should be requested in increasing order.")
    lastPartitionId = reducePartitionId
    if (outputTempFile == null) outputTempFile = Utils.tempFileWith(outputFile)
    if (outputFileChannel != null) currChannelPosition = outputFileChannel.position
    else currChannelPosition = getPartitionOffset(reducePartitionId)
    new NvkvShufflePartitionWriter(reducePartitionId)
  }

  @throws[IOException]
  override def commitAllPartitions: Array[Long] = {
    NvkvShuffleMapOutputWriter.log.info("NvkvShuffleMapOutputWriter commitAllPartitions")
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
    if (outputFileChannel != null && outputFileChannel.position != bytesWrittenToMergedFile) throw new IOException("Current position " + outputFileChannel.position + " does not equal expected " + "position " + bytesWrittenToMergedFile + " after transferTo. Please check your " + " kernel version to see if it is 2.6.32, as there is a kernel bug which will lead " + "to unexpected behavior when using transferTo. You can set " + "spark.file.transferTo=false to disable this NIO feature.")
    cleanUp()
    val resolvedTmp = if (outputTempFile != null && outputTempFile.isFile) outputTempFile
    else null

    var packMapperData: ByteBuffer = ByteBuffer.allocateDirect(8 + 8 + 4 + 2*8*partitionLengths.size).order(ByteOrder.nativeOrder())

    packMapperData.putInt(1)
    packMapperData.putInt(partitionLengths.size)
    packMapperData.putInt(mapId.toInt)

    partitionLengths.zip(0 until partitionLengths.size).foreach{ 
        case (partitionLength, reduceId) => {
            var partitionOffset = getPartitionOffset(reduceId);
            NvkvShuffleMapOutputWriter.log.info(s"shuffleId $shuffleId mapId $mapId reducerId $reduceId offset $partitionOffset size $partitionLength")
            NvkvShuffleMapOutputWriter.log.info(s"padding ${partitionsPadding(reduceId)} offset ${nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId)} length ${nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId)}")
            NvkvShuffleMapOutputWriter.log.info(s"Send DPU AM: shuffleId $shuffleId mapId $mapId reducerId $reduceId offset ${nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId)} size ${nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId)}")
            //TODO - move to shuffleTransport
            packMapperData.putLong(nvkvHandler.getPartitonOffset(shuffleId, mapId, reduceId))
            packMapperData.putLong(nvkvHandler.getPartitonLength(shuffleId, mapId, reduceId))
            // partitionOffset += partitionLength
        }
    }

    packMapperData.rewind
    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    ucxTransport.commitBlock(1, resultBufferAllocator, packMapperData)
    NvkvShuffleMapOutputWriter.log.info("Writing shuffle index file for mapId " + mapId + " with lengths " + partitionLengths(0) + " " + partitionLengths(1))
    ucxTransport.progress()
    blockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, resolvedTmp)
    partitionLengths
  }

  @throws[IOException]
  override def abort(error: Throwable): Unit = {
    cleanUp()
    if (outputTempFile != null && outputTempFile.exists && !outputTempFile.delete) {
    }
  }

  @throws[IOException]
  private def cleanUp(): Unit = {
    NvkvShuffleMapOutputWriter.log.info("NvkvShuffleMapOutputWriter cleanUp")
    if (outputBufferedFileStream != null) outputBufferedFileStream.close()
    if (outputFileChannel != null) outputFileChannel.close()
    if (outputFileStream != null) outputFileStream.close()
  }

  @throws[IOException]
  private def initStream(): Unit = {
    NvkvShuffleMapOutputWriter.log.info(s"NvkvShuffleMapOutputWriter initStream bufferSize $bufferSize")
    if (outputFileStream == null) outputFileStream = new FileOutputStream(outputTempFile, true)
    if (outputBufferedFileStream == null) outputBufferedFileStream = new BufferedOutputStream(outputFileStream, bufferSize)
  }

  @throws[IOException]
  private def initChannel(): Unit = {
    NvkvShuffleMapOutputWriter.log.info("NvkvShuffleMapOutputWriter initChannel")
    // This file needs to opened in append mode in order to work around a Linux kernel bug that
    // affects transferTo; see SPARK-3948 for more details.
    if (outputFileChannel == null) outputFileChannel = new FileOutputStream(outputTempFile, true).getChannel
  }

  private class NvkvShufflePartitionWriter (private val partitionId: Int) extends ShufflePartitionWriter {
    NvkvShuffleMapOutputWriter.log.info("NvkvShufflePartitionWriter ctor " + partitionId + " shuffleId " + shuffleId + " mapId " + mapId)
    private var partStream: PartitionWriterStream = null
    private var partChannel: PartitionWriterChannel = null

    @throws[IOException]
    override def openStream: OutputStream = {
      NvkvShuffleMapOutputWriter.log.info("NvkvShufflePartitionWriter openStream " + partitionId)
      if (partStream == null) {
        if (outputFileChannel != null) throw new IllegalStateException("Requested an output channel for a previous write but" + " now an output stream has been requested. Should not be using both channels" + " and streams to write.")
        initStream()
        partStream = new PartitionWriterStream(partitionId)
      }
      partStream
    }

    @throws[IOException]
    override def openChannelWrapper: Optional[WritableByteChannelWrapper] = {
      NvkvShuffleMapOutputWriter.log.info("NvkvShufflePartitionWriter openChannelWrapper " + partitionId)
      if (partChannel == null) {
        if (partStream != null) throw new IllegalStateException("Requested an output stream for a previous write but" + " now an output channel has been requested. Should not be using both channels" + " and streams to write.")
        initChannel()
        partChannel = new PartitionWriterChannel(partitionId)
      }
      Optional.of(partChannel)
    }

    override def getNumBytesWritten: Long = {
      NvkvShuffleMapOutputWriter.log.info("NvkvShufflePartitionWriter getNumBytesWritten " + partitionId)
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
    NvkvShuffleMapOutputWriter.log.info("PartitionWriterStream ctor " + partitionId)
    private var count = 0
    private var isClosed = false

    def getCount: Long = count

    @throws[IOException]
    override def write(b: Int): Unit = {
      NvkvShuffleMapOutputWriter.log.info("PartitionWriterStream write1 " + b)
      verifyNotClosed()
      outputBufferedFileStream.write(b)
      count += 1
    }

    @throws[IOException]
    override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
      val offset = currChannelPosition + count
      NvkvShuffleMapOutputWriter.log.info(s"PartitionWriterStream write2 $shuffleId,$mapId,$partitionId buf $buf pos $pos length $length offset $offset")
      nvkvHandler.write(shuffleId, mapId, partitionId, buf, length, offset)
      verifyNotClosed()
      outputBufferedFileStream.write(buf, pos, length)
      count += length
    }

    override def close(): Unit = {
      var padding: Int = nvkvHandler.writeRemaining(currChannelPosition+count)
      nvkvHandler.commitPartition(currChannelPosition, count, shuffleId, mapId, partitionId)
      isClosed = true
      partitionLengths(partitionId) = count
      totalPartitionsPadding += padding
      partitionsPadding(partitionId) = padding
      bytesWrittenToMergedFile += count
      NvkvShuffleMapOutputWriter.log.info(s"PartitionWriterStream close1 currChannelPosition ${currChannelPosition}")
      NvkvShuffleMapOutputWriter.log.info(s"PartitionWriterStream close1 $shuffleId,$mapId,$partitionId count $count padding $padding bytesWrittenToMergedFile $bytesWrittenToMergedFile")
    }

    private def verifyNotClosed(): Unit = {
      if (isClosed) throw new IllegalStateException("Attempting to write to a closed block output stream.")
    }
  }

  private class PartitionWriterChannel private[ucx](private val partitionId: Int) extends WritableByteChannelWrapper {
    NvkvShuffleMapOutputWriter.log.info("PartitionWriterChannel ctor " + partitionId)

    @throws[IOException]
    def getCount: Long = {
      NvkvShuffleMapOutputWriter.log.info("PartitionWriterChannel getCount " + partitionId)
      val writtenPosition = outputFileChannel.position
      writtenPosition - currChannelPosition
    }

    override def channel: WritableByteChannel = {
      NvkvShuffleMapOutputWriter.log.info("PartitionWriterChannel channel " + partitionId)
      outputFileChannel
    }

    @throws[IOException]
    override def close(): Unit = {
      NvkvShuffleMapOutputWriter.log.info("PartitionWriterStream close2")
      partitionLengths(partitionId) = getCount
      bytesWrittenToMergedFile += partitionLengths(partitionId)
    }
  }
}
