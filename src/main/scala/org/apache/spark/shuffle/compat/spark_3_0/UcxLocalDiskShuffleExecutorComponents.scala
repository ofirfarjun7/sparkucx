/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.utils.CommonUtils
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleExecutorComponents, LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter}
import org.apache.spark.shuffle.UcxShuffleManager
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}

/**
 * Entry point to UCX executor.
 */
class UcxLocalDiskShuffleExecutorComponents(sparkConf: SparkConf)
  extends LocalDiskShuffleExecutorComponents(sparkConf) with Logging{

  private var blockResolver: UcxShuffleBlockResolver = _

  override def initializeExecutor(appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = {
    logDebug("LEO UcxLocalDiskShuffleExecutorComponents initializeExecutor appId: " + appId + " execId: " + execId)
    val ucxShuffleManager = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
    CommonUtils.safePolling(() => {},
      () => {ucxShuffleManager.ucxTransport == null}, 10*1000,
      new CommonUtils.CommonUtilsTimeoutException(s"Got timeout when polling"), 5)

    blockResolver = ucxShuffleManager.shuffleBlockResolver
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
    // Not used
    logDebug("LEO UcxLocalDiskShuffleExecutorComponents createMapOutputWriter shuffleId: " + shuffleId + " mapTaskId: " + mapTaskId + " numPartitions: " + numPartitions)
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }
    new LocalDiskShuffleMapOutputWriter(
      shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf)
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long): Optional[SingleSpillShuffleMapOutputWriter] = {
    // Called per each mapper
    logDebug("LEO UcxLocalDiskShuffleExecutorComponents createSingleFileMapOutputWriter shuffleId: " + shuffleId + " mapId: " + mapId)
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }

    // Need to implement an alternative to LocalDiskSingleSpillMapOutputWriter?
    Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver))
  }

}
