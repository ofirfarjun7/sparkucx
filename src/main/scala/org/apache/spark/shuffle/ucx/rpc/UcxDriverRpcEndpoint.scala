/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints: mutable.Set[RpcEndpointRef] = mutable.HashSet.empty
  private var executorToDpuAddress = HashMap.empty[Long, SerializableDirectBuffer]


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(executorId: Long, endpoint: RpcEndpointRef,
    dpuSockAddress: SerializableDirectBuffer) => {
      // Driver receives a message from executor with it's local dpu sockAddress
      // 1. Introduce existing members of a cluster
      logDebug(s"LEO driver received $message")
      logDebug(s"LEO dpuSockAddress: ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)}")
      if (executorToDpuAddress.nonEmpty) {
        val msg = IntroduceAllExecutors(executorToDpuAddress)
        logDebug(s"replying $msg to $executorId")
        context.reply(msg)
      }
      executorToDpuAddress += executorId -> dpuSockAddress
      // 2. For each existing member introduce newly joined executor.
      endpoints.foreach(ep => {
        logDebug(s"Sending $message to $ep")
        ep.send(message)
      })
      logDebug(s"Connecting back to address: ${context.senderAddress}")
      endpoints.add(endpoint)
    }
  }
}
