/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.utils


import java.io.File;
import java.net.InetSocketAddress
import scala.collection.JavaConverters._
import org.ini4j.Ini
import org.apache.spark.internal.Logging

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
    def getLocalDpuSocketAddress(): InetSocketAddress = {
      val port = 1338
      val hostname = java.net.InetAddress.getLocalHost.getHostName.split("\\.")(0)
      val ipAddress = dpusSection.get(hostname)
      new InetSocketAddress(ipAddress, port)
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