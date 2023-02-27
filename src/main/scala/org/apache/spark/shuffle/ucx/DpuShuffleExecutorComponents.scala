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

import org.openucx.nvkv.Nvkv;

// temp
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter};

class NvkvShuffleExecutorComponents(val sparkConf: SparkConf) extends ShuffleExecutorComponents with Logging {
    logDebug("LEO NvkvShuffleExecutorComponents constructor");
    // Need to pass disk+nsid in Init
    // Nvkv.init("mlx5_0", 1, "0x1");

  private var blockResolver: IndexShuffleBlockResolver = null;

  override def initializeExecutor(appId: String, execId: String, extraConfigs: Map[String, String]) = {
    logDebug("LEO NvkvShuffleExecutorComponents initializeExecutor");
    val blockManager = SparkEnv.get.blockManager;
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int) = {
    logDebug("LEO NvkvShuffleExecutorComponents createMapOutputWriter");
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    new NvkvShuffleMapOutputWriter(shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long) = {
    Optional.empty();

   }
}