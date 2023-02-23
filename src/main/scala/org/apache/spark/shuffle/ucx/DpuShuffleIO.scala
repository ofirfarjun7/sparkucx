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
    // logDebug("LEO DpuShuffleIO constructor");

    // def driver() = {
    //     new ShuffleDriverComponents();
    // }

 private val sparkConf = conf;

//   LocalDiskShuffleDataIO(SparkConf sparkConf) {
//     this.sparkConf = sparkConf;
//   }

//   @Override
override def executor() = {
    logDebug("LEO DpuShuffleIO executor");
    new DpuShuffleExecutorComponents(sparkConf);
  }

//   @Override
override def driver() = {
    logDebug("LEO DpuShuffleIO driver");
    new LocalDiskShuffleDriverComponents();
  }
}

    // public DpuShuffleIO() {
    //     logDebug("LEO DpuShuffleIO constructor");
    // }

    // public ShuffleExecutorComponents executor() {
    //     logDebug("LEO DpuShuffleIO executor");
    //     return new DpuShuffleExecutorComponents();
    // }

    // public ShuffleDriverComponents driver() {
    //     logDebug("LEO DpuShuffleIO driver");
    //     return new DpuShuffleDriverComponents();
    // }

