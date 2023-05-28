/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;


import java.util.Map;
import java.util.Optional;

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;
// import org.apache.spark.shuffle.ucx.DpuShuffleMapOutputWriter;
import org.apache.spark.shuffle.ucx;

import org.openucx.jnvkv.NvkvHandler;

// temp
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter};

class NvkvShuffleExecutorComponents(val sparkConf: SparkConf, val ucxTransport: UcxShuffleTransport) extends ShuffleExecutorComponents with Logging {
  logDebug("LEO NvkvShuffleExecutorComponents constructor");

  private var blockResolver: IndexShuffleBlockResolver = null
  private var nvkvHandler: NvkvHandler = null
  private var transport: UcxShuffleTransport = ucxTransport


  override def initializeExecutor(appId: String, execId: String, extraConfigs: Map[String, String]) = {
    logDebug("LEO NvkvShuffleExecutorComponents initializeExecutor");
    val blockManager = SparkEnv.get.blockManager;
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
    logDebug("LEO NvkvShuffleExecutorComponents initializeExecutor init NvkvHandler");
    //TODO - pass number of executors.
    nvkvHandler = NvkvHandler.getHandler(1)
    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    transport.initExecuter(1, UcxShuffleBlockId(1, 1, 0), resultBufferAllocator)
    transport.progress()
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int) = {
    logDebug("LEO NvkvShuffleExecutorComponents createMapOutputWriter");
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    new NvkvShuffleMapOutputWriter(shuffleId, mapTaskId, numPartitions, nvkvHandler, blockResolver, sparkConf);
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long) = {
    Optional.empty();

   }
}