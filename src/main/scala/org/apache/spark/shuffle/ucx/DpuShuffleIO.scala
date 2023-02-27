/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
// import org.apache.spark.shuffle.ucx.DpuShuffleExecutorComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;


class DpuShuffleIO(val conf: SparkConf) extends ShuffleDataIO with Logging {
    logDebug("LEO DpuShuffleIO constructor");


 private val sparkConf = conf;

//   LocalDiskShuffleDataIO(SparkConf sparkConf) {
//     this.sparkConf = sparkConf;
//   }

//   @Override
override def executor() = {
    throw new UnsupportedOperationException("Not implemented for DPU");
    logDebug("LEO DpuShuffleIO executor");
    // TODO: Why this one is never called?
    new DpuShuffleExecutorComponents(sparkConf);
  }

//   @Override
override def driver() = {
    logDebug("LEO DpuShuffleIO driver");
    // TODO: Need to rewrite this one?
    new LocalDiskShuffleDriverComponents();
  }
}
