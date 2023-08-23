/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors, NvkvLock, NvkvReleaseLock, NvkvRequestLock}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

import java.util.concurrent.ExecutorService

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, var transport: UcxShuffleTransport,
                             executorService: ExecutorService, var ucxTransport: UcxShuffleTransport,
                             driverEndpoint: RpcEndpointRef, executorId: Long, nvkvInitCb: (UcxShuffleTransport) => Unit)
  extends RpcEndpoint  with Logging {

  override def receive: PartialFunction[Any, Unit] = {

    case NvkvLock(executerLocalId: Int) => {
      logInfo(s"NvkvLock: Executer $executorId receive Nvkv Lock")
      transport.initNvkv(executerLocalId)
      nvkvInitCb(transport)
      logInfo(s"NvkvLock: Executer $executorId release Nvkv Lock")
      driverEndpoint.send(NvkvReleaseLock(executorId))
    }

    case ExecutorAdded(executorId: Long, _: RpcEndpointRef,
    dpuSockAddress: SerializableDirectBuffer) =>
      logDebug(s"Executor received ExecutorAdded($executorId)")
      logDebug(s"dpuSockAddress: ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)}")
      executorService.submit(new Runnable() {
        override def run(): Unit = {
          logDebug(s"ExecutorRpc transport.addExecutor($executorId, ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)})")
          transport.addExecutor(executorId, dpuSockAddress.value)
        }
      })
    case IntroduceAllExecutors(executorIdToDpuAddresses: Map[Long, SerializableDirectBuffer]) =>
      logDebug(s"Received IntroduceAllExecutors(${executorIdToDpuAddresses.keys.mkString(",")}")
      executorService.submit(new Runnable() {
        override def run(): Unit = {
          logDebug(s"ExecutorRpc transport.addExecutors(executorIdToDpuAddresses)")
          transport.addExecutors(executorIdToDpuAddresses)
          transport.preConnect()
        }
      })
  }
}
