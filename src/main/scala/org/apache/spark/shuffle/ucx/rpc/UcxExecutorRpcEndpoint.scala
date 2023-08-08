/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

import java.util.concurrent.ExecutorService

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransport,
                             executorService: ExecutorService)
  extends RpcEndpoint  with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(executorId: Long, _: RpcEndpointRef,
    dpuSockAddress: SerializableDirectBuffer) =>
      logDebug(s"LEO executor received ExecutorAdded($executorId)")
      logDebug(s"LEO dpuSockAddress: ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)}")
      executorService.submit(new Runnable() {
        override def run(): Unit = {
          logDebug(s"LEO ExecutorRpc transport.addExecutor($executorId, ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)})")
          transport.addExecutor(executorId, dpuSockAddress.value)
        }
      })
    case IntroduceAllExecutors(executorIdToDpuAddresses: Map[Long, SerializableDirectBuffer]) =>
      logDebug(s"LEO received IntroduceAllExecutors(${executorIdToDpuAddresses.keys.mkString(",")}")
      executorService.submit(new Runnable() {
        override def run(): Unit = {
          logDebug(s"LEO ExecutorRpc transport.addExecutors(executorIdToDpuAddresses)")
          transport.addExecutors(executorIdToDpuAddresses)
          transport.preConnect()
        }
      })
  }
}
