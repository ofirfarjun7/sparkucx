/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ucx.rpc.{UcxDriverRpcEndpoint, UcxExecutorRpcEndpoint}
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils, DpuUtils}
import org.apache.spark.shuffle.utils.{UnsafeUtils, CommonUtils}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
abstract class CommonUcxShuffleManager(val conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int
  type ReduceId = Long

  val ucxShuffleConf = new UcxShuffleConf(conf)

  @volatile var ucxTransport: UcxShuffleTransport = _

  private var executorEndpoint: UcxExecutorRpcEndpoint = _
  private var driverEndpoint: UcxDriverRpcEndpoint = _

  protected val driverRpcName = "SparkUCX_driver"

  private val setupThread = ThreadUtils.newDaemonSingleThreadExecutor("UcxTransportSetupThread")

  logDebug("LEO CommonUcxShuffleManager")

  def getTransport(): UcxShuffleTransport = { ucxTransport }

  setupThread.submit(new Runnable {
    override def run(): Unit = {
      CommonUtils.safePolling(() => {},
      () => {SparkEnv.get == null}, 10*1000,
      new CommonUtils.CommonUtilsTimeoutException(s"Got timeout when polling"), 10)

      if (isDriver) {
        val rpcEnv = SparkEnv.get.rpcEnv
        logInfo(s"Setting up driver RPC")
        driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(driverRpcName, driverEndpoint)
      } else {
        CommonUtils.safePolling(() => {},
          () => {SparkEnv.get.blockManager.blockManagerId == null}, 10*1000,
          new CommonUtils.CommonUtilsTimeoutException(s"Got timeout when polling"), 5)
        startUcxTransport()
      }
    }
  })

  /**
   * Atomically starts UcxNode singleton - one for all shuffle threads.
   */
  def startUcxTransport(): Unit = if (ucxTransport == null) {
    logInfo(s"LEO startUcxTransport")
    val blockManager = SparkEnv.get.blockManager.blockManagerId
    val transport = new UcxShuffleTransport(ucxShuffleConf, blockManager.executorId.toLong)
    transport.init()

    val rpcEnv = RpcEnv.create("ucx-rpc-env", blockManager.host, blockManager.port,
      conf, new SecurityManager(conf), clientMode = false)
    executorEndpoint = new UcxExecutorRpcEndpoint(rpcEnv, transport, setupThread)
    val endpoint = rpcEnv.setupEndpoint(
      s"ucx-shuffle-executor-${blockManager.executorId}",
      executorEndpoint)
    val driverEndpoint = RpcUtils.makeDriverRef(driverRpcName, conf, rpcEnv)
    logInfo(s"LEO startUcxTransport sending RPC IntroduceAllExecutors")

    val dpuAddress = DpuUtils.getLocalDpuAddress().getBytes(StandardCharsets.UTF_8)
    val address = ByteBuffer.allocateDirect(dpuAddress.length + 4)
    address.putInt(1338)
    address.put(dpuAddress)
    transport.addExecutor(blockManager.executorId.toLong, address)
    transport.preConnect()

    var sockAddress = new InetSocketAddress(DpuUtils.getLocalDpuAddress(), 1338)
    logInfo(s"LEO sockAddress ${sockAddress} host ${sockAddress.getHostName} address ${sockAddress.getAddress}")
    driverEndpoint.ask[IntroduceAllExecutors](ExecutorAdded(blockManager.executorId.toLong, endpoint,
      new SerializableDirectBuffer(SerializationUtils.serializeInetAddress(sockAddress))))
      .andThen {
        case Success(msg) =>
          logInfo(s"Receive reply $msg")
          executorEndpoint.receive(msg)
    }

    ucxTransport = transport
  }


  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.asInstanceOf[CommonUcxShuffleBlockResolver].removeShuffle(shuffleId)
    super.unregisterShuffle(shuffleId)
  }

  /**
   * Called on both driver and executors to finally cleanup resources.
   */
  override def stop(): Unit = synchronized {
    super.stop()
    if (ucxTransport != null) {
      ucxTransport.close()
      ucxTransport = null
    }
    if (executorEndpoint != null) {
      executorEndpoint.stop()
    }
    if (driverEndpoint != null) {
      driverEndpoint.stop()
    }
    setupThread.shutdown()
  }

}
