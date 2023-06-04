/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.compat.spark_3_0.{UcxShuffleBlockResolver, UcxShuffleReader}
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleWriter, UnsafeShuffleWriter}
import org.apache.spark.shuffle.ucx.{CommonUcxShuffleManager, NvkvShuffleExecutorComponents}
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}

/**
 * Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
 * and injects needed logic in override methods.
 */

 /**
 TODOs
 1. Check how Spark decides when to spill reduce blocks to the HD, 
    and how we can use it to improve performance.
 */
class UcxShuffleManager(override val conf: SparkConf, isDriver: Boolean)
  extends CommonUcxShuffleManager(conf, isDriver) {

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver = new UcxShuffleBlockResolver(this)

  override def getWriter[K, V](handle: ShuffleHandle, mapId: ReduceId, context: TaskContext,
                               metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logDebug("LEO UcxShuffleManager getWriter")
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K@unchecked, V@unchecked] =>
        logDebug("LEO UcxShuffleManager getWriter unsafeShuffleHandle")
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case other: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        logDebug("LEO UcxShuffleManager getWriter other")
        new SortShuffleWriter(
          shuffleBlockResolver, other, mapId, context, shuffleExecutorComponents)
    }
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: MapId, endPartition: MapId,
                               context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    logDebug("LEO UcxShuffleManager getReader")
    new UcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K,_,C]], startPartition, endPartition,
      context, ucxTransport, readMetrics = metrics, shouldBatchFetch = false)
  }

  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    logDebug("LEO UcxShuffleManager loadShuffleExecutorComponents")
    val executorComponents = new NvkvShuffleExecutorComponents(conf, ucxTransport)
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
      .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

  @Override
  override def stop() = {
    logDebug("LEO UcxShuffleManager - Closing nvkv")
    // Nvkv.fini()
    super.stop()
  }
}
