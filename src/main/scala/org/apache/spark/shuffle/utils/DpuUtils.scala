/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.utils

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import java.io.File;
import org.ini4j.Ini

object DpuUtils extends Logging {
    val ini: Ini =
      try {
        val CLUSTER_CONF_FILE = "/hpc/mtr_scrap/users/ofarjon/spark/cluster.ini"
        new Ini(new File(CLUSTER_CONF_FILE))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          new Ini()
      }
    val dpusSection = ini.get("dpu")
    /**
      * Retrieve the IP address of the local DPU. For POC purposes, all IPs are assumed
      * to be in a network mounted file. This logic will be later replaced.
      *
      * @return
      */
    def getLocalDpuAddress(): String = {
      val hostname = java.net.InetAddress.getLocalHost.getHostName.split("\\.")(0)
      dpusSection.get(hostname)
    }

    /**
      * @return Number of DPUs in the cluster
      */
    def getNumberOfDpus(): Int = {
      var numOfDpus: Int = 0
      for (dpu <- dpusSection.keySet().asScala) {
        numOfDpus += 1
      }

      numOfDpus
    }
}