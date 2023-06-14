/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.utils

import org.apache.spark.internal.Logging


import java.io.File;
import org.ini4j.Ini

object DpuUtils {
    val CLUSTER_CONF_FILE = "/hpc/mtr_scrap/users/ofarjon/spark/cluster.ini"

    /**
      * Retrieve the IP address of the local DPU. For POC purposes, all IPs are assumed
      * to be in a network mounted file. This logic will be later replaced.
      *
      * @return
      */
    def getLocalDpuAddress(): String = {
        val ini = new Ini(new File(CLUSTER_CONF_FILE))
        val section = ini.get("dpu")
        // logDebug(s"LEO section: $section")
        val hostname = java.net.InetAddress.getLocalHost.getHostName.split("\\.")(0)
        // logDebug(s"LEO hostname: $hostname")
        val dpuAddress = section.get(hostname)
        System.err.println(s"LEO dpuAddress: $dpuAddress")

        dpuAddress
    }
}