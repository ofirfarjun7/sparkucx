/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.net.InetSocketAddress

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors, NvkvRequestLock,
    NvkvReleaseLock, NvkvLock}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints: mutable.Set[RpcEndpointRef] = mutable.HashSet.empty
  private var executorToDpuAddress = HashMap.empty[Long, SerializableDirectBuffer]
  private var nvkvLock = new TrieMap[InetSocketAddress, Int]
  private var nvkvLockPendingQMap = new TrieMap[InetSocketAddress, mutable.ListBuffer[RpcEndpointRef]]
  
  override def receive: PartialFunction[Any, Unit] = {
    case message@NvkvReleaseLock(executorId: Long) => {
      logInfo(s"NvkvLock: executor $executorId release lock")
      if (executorToDpuAddress.contains(executorId)) {
        val execAdd = executorToDpuAddress(executorId)
        val desExecAdd = SerializationUtils.deserializeInetAddress(execAdd.value)
        var nvkvLockPendingQ = nvkvLockPendingQMap.getOrElseUpdate(desExecAdd,
                                  new mutable.ListBuffer[RpcEndpointRef])
        if (nvkvLockPendingQ.isEmpty) {
          nvkvLock.put(desExecAdd, 0)
        } else {
          val pendingEp = nvkvLockPendingQ.remove(0)
          logInfo(s"NvkvLock: send lock to pending")
          pendingEp.send(new NvkvLock(1))
        }
      } else {
        throw new IllegalStateException(
          "Executer must be signed to driver EP before releasing the lock")
      }
    }


    case message@NvkvRequestLock(executorId: Long, execEp: RpcEndpointRef) => {
      logInfo(s"NvkvLock: executor $executorId request lock")
      if (executorToDpuAddress.contains(executorId)) {
        val execAdd = executorToDpuAddress(executorId)
        val desExecAdd = SerializationUtils.deserializeInetAddress(execAdd.value)
        var tryLock = nvkvLock.replace(desExecAdd, 0, 1)
        if (tryLock) {
          execEp.send(new NvkvLock(1))
          logInfo(s"NvkvLock: Lock given to $executorId")
        } else {
          logInfo(s"NvkvLock: Lock is not free, $executorId is pending...")
          var nvkvLockPendingQ = nvkvLockPendingQMap.getOrElseUpdate(desExecAdd, 
                                    new mutable.ListBuffer[RpcEndpointRef])
          nvkvLockPendingQ.append(execEp)
        }
      } else {
        throw new IllegalStateException(
        "Executer must be signed in driver EP before requesting the lock")
      }
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(executorId: Long, endpoint: RpcEndpointRef,
    dpuSockAddress: SerializableDirectBuffer) => {
      // Driver receives a message from executor with it's local dpu sockAddress
      // 1. Introduce existing members of a cluster
      logDebug(s"driver received $message")
      logDebug(s"dpuSockAddress: ${SerializationUtils.deserializeInetAddress(dpuSockAddress.value)}")
      if (executorToDpuAddress.nonEmpty) {
        val msg = IntroduceAllExecutors(executorToDpuAddress)
        logDebug(s"replying $msg to $executorId")
        context.reply(msg)
      }
      executorToDpuAddress += executorId -> dpuSockAddress

      nvkvLock.getOrElseUpdate(SerializationUtils.deserializeInetAddress(dpuSockAddress.value), {0})
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
