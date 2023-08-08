/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.io.Closeable
import java.util.concurrent.{ConcurrentLinkedQueue, Callable, Future, FutureTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxBounceBufferMemoryBlock
import org.apache.spark.shuffle.ucx.utils.{SerializationUtils, UcpSparkAmId}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ThreadUtils

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.parallel.ForkJoinTaskSupport

class UcxFailureOperationResult(errorMsg: String) extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.FAILURE

  override def getError: TransportError = new TransportError(errorMsg)

  override def getStats: Option[OperationStats] = None

  override def getData: MemoryBlock = null
}

class UcxAmDataMemoryBlock(ucpAmData: UcpAmData, offset: Long, size: Long,
                           refCount: AtomicInteger)
  extends MemoryBlock(ucpAmData.getDataAddress + offset, size, true) with Logging {

  override def close(): Unit = {
    if (refCount.decrementAndGet() == 0) {
      ucpAmData.close()
    }
  }
}

class UcxRefCountMemoryBlock(baseBlock: MemoryBlock, offset: Long, size: Long,
                             refCount: AtomicInteger)
  extends MemoryBlock(baseBlock.address + offset, size, true) with Logging {

  override def close(): Unit = {
    if (refCount.decrementAndGet() == 0) {
      baseBlock.close()
    }
  }
}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, id: Long = 0L)
  extends Closeable with Logging {

  private final val connections =  new TrieMap[transport.ExecutorId, UcpEndpoint]
  private final val dpuAddress =  new TrieMap[InetSocketAddress, UcpEndpoint]
  private val requestData = new TrieMap[Int, (Seq[OperationCallback], () => Unit, UcxRequest, transport.BufferAllocator)]
  private val tag = new AtomicInteger(Random.nextInt())
  private val flushRequests = new ConcurrentLinkedQueue[UcpRequest]()

  private val ioThreadPool = ThreadUtils.newForkJoinPool("IO threads",
    transport.ucxShuffleConf.numIoThreads)
  private val ioTaskSupport = new ForkJoinTaskSupport(ioThreadPool)

  private var connectToRemoteNvkv = false
  private var requestComplete = false

  private class ProgressThread(id: Long) extends Thread {
    private val useWakeup = transport.ucxShuffleConf.useWakeup
    private val taskQueue = new ConcurrentLinkedQueue[Runnable]()
    setDaemon(true)
    setName(s"UCX-worker $id")

    override def run (): Unit = {
      logDebug(s"UCX-worker $id started")
      while (!isInterrupted) {
        Option(taskQueue.poll()) match {
          case Some(task) => task.run()
          case None => ()
        }
        while(worker.progress() != 0) {}
        if (taskQueue.isEmpty && useWakeup) {
          worker.waitForEvents()
        }
      }
      logDebug(s"UCX-worker $id stopped")
    }

    @inline
    def submit(task: Callable[_]): Future[_] = {
      val future = new FutureTask(task)
      taskQueue.offer(future)
      worker.signal()
      future
    }

    @inline
    def submit(task: Runnable): Future[Unit.type] = {
      val future = new FutureTask(task, Unit)
      taskQueue.offer(future)
      worker.signal()
      future
    }
  }

  private val progressThread = new ProgressThread(id)

  worker.setAmRecvHandler(UcpSparkAmId.InitExecutorAck,
  (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {

    logDebug(s"LEO InitExecutorAck called!")

    if (ucpAmData.isDataValid) {
      // val buff: ByteBuffer = UcxUtils.getByteBufferView(ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
      val buf: ByteBuffer = UcxUtils.getByteBufferView(ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
      var bytes: Array[Byte] = new Array[Byte](ucpAmData.getLength.toInt);
      buf.get(bytes);
      transport.getNvkvHandler.connectToRemote(bytes)
      connectToRemoteNvkv = true
    } else {
      logDebug(s"LEO InitExecutorAck handler Invalid data!!!")  
    }

    logDebug(s"LEO Connected to remote nvkv on DPU")
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  worker.setAmRecvHandler(UcpSparkAmId.FetchBlockReqAck,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt).order(ByteOrder.nativeOrder())
      val i = headerBuffer.getInt
      val data = requestData.remove(i)

      logDebug(s"LEO Fetch block ack called!")

      if (data.isEmpty) {
        throw new UcxException(s"No data for tag $i.")
      }

      val (callbacks, amRecvStartCb, request, allocator) = data.get
      val stats = request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize = ucpAmData.getLength

      amRecvStartCb()
      val refCounts = new AtomicInteger(1)
      if (ucpAmData.isDataValid) {
        request.completed = true
        stats.endTime = System.nanoTime()
        logDebug(s"Received block with length ${ucpAmData.getLength} in ${stats.getElapsedTimeNs} ns")

        if (callbacks(0) != null) {
          callbacks(0).onComplete(new OperationResult {
            override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

            override def getError: TransportError = null

            override def getStats: Option[OperationStats] = Some(stats)

            override def getData: MemoryBlock = new UcxAmDataMemoryBlock(ucpAmData, 0, stats.receiveSize, refCounts)
          })
        }
        if (callbacks.isEmpty) UcsConstants.STATUS.UCS_OK else UcsConstants.STATUS.UCS_INPROGRESS

      } else {
        logDebug(s"LEO Received RNDV rts Length: ${stats.receiveSize}")
        val mem = allocator(ucpAmData.getLength)
        stats.amHandleTime = System.nanoTime()

        request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                request.completed = true
                stats.endTime = System.nanoTime()
                logDebug(s"Perftest receive tag $i time ${System.nanoTime()} size ${mem.size}")
                logDebug(s"Received rndv data of size: ${mem.size} for tag $i in " +
                  s"${stats.getElapsedTimeNs} ns " +
                  s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
                  callbacks(0).onComplete(new OperationResult {
                    override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

                    override def getError: TransportError = null

                    override def getStats: Option[OperationStats] = Some(stats)

                    override def getData: MemoryBlock = new UcxRefCountMemoryBlock(mem, 0, stats.receiveSize, refCounts)
                  })
              }
            }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
        UcsConstants.STATUS.UCS_OK
      }
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  @inline
  def start(): Unit = progressThread.start()

  override def close(): Unit = {
    progressThread.interrupt()
    progressThread.join(10)
    val closeRequests = connections.map {
      case (_, endpoint) => endpoint.closeNonBlockingForce()
    }
    while (!closeRequests.forall(_.isCompleted)) {
      progress()
    }
    ioThreadPool.shutdown()
    connections.clear()
    worker.close()
  }

  @inline
  def addExecutor(executorId: transport.ExecutorId) = {
    progressThread.submit(new Runnable {
      override def run = {
        getConnection(executorId)
        progressConnect()
      }
    })
  }

  @inline
  def addExecutors() = {
    progressThread.submit(new Runnable {
      override def run = {
        preconnect()
      }
    })
  }

  /**
   * Blocking progress until there's outstanding flush requests.
   */
  private def progressConnect(): Unit = {
    while (!flushRequests.isEmpty) {
      progress()
      flushRequests.removeIf(_.isCompleted)
    }
    logTrace(s"Flush completed. Number of connections: ${connections.keys.size}")
  }

  /**
   * The only place for worker progress
   */
  def progress(): Int = worker.synchronized {
    worker.progress()
  }

  /**
   * Establish connections to known instances.
   */
  private def preconnect(): Unit = {
    transport.executorAddresses.keys.foreach(getConnection)
    progressConnect()
  }

  // def connectByWorkerAddress(executorId: transport.ExecutorId, workerAddress: ByteBuffer): Unit = {
  //   logDebug(s"Worker $this connecting back to $executorId by worker address")
  //   val ep = worker.newEndpoint(new UcpEndpointParams().setName(s"Server connection to $executorId")
  //     .setUcpAddress(workerAddress))
  //   connections.put(executorId, ep)
  // }

  private def getConnection(executorId: transport.ExecutorId): UcpEndpoint = {
    // TODO: Skip connection if already connected to the DPU of this executor

    // val startTime = System.currentTimeMillis()
    // while (!transport.executorAddresses.contains(executorId)) {
    //   if  (System.currentTimeMillis() - startTime >
    //     transport.ucxShuffleConf.getSparkConf.getTimeAsMs("spark.network.timeout", "100")) {
    //     throw new UcxException(s"Don't get a worker address for $executorId")
    //   }
    // }

    connections.getOrElseUpdate(executorId,  {
      val address = transport.executorAddresses(executorId)
      val desAddress = SerializationUtils.deserializeInetAddress(address)

      dpuAddress.getOrElseUpdate(desAddress, {
        val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
          .setSocketAddress(desAddress).sendClientId()
          .setErrorHandler(new UcpEndpointErrorHandler() {
            override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
              logError(s"Endpoint to $executorId got an error: $errorMsg")
              dpuAddress.remove(desAddress)
            }
          }).setName(s"Endpoint to DPU/$desAddress")

        logDebug(s"Worker $this connecting to DPU($desAddress)")
        val ep = worker.newEndpoint(endpointParams)
        val header = Platform.allocateDirectBuffer(UnsafeUtils.LONG_SIZE)
        header.putLong(id)
        header.rewind()
        val workerAddress = worker.getAddress
        ep
      })
    })
  }

  @inline
  def fetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: transport.BufferAllocator,
                            callbacks: Seq[OperationCallback],
                            amRecvStartCb: () => Unit): Unit = {
    progressThread.submit(new Runnable {
      override def run = {
        doFetchBlocksByBlockIds(executorId, blockIds, resultBufferAllocator, callbacks, amRecvStartCb)
      }
    })
  }

  def doFetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                              resultBufferAllocator: transport.BufferAllocator,
                              callbacks: Seq[OperationCallback],
                              amRecvStartCb: () => Unit): Unit = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE
    val ep = getConnection(executorId)

    // if (worker.getMaxAmHeaderSize <=
    //   headerSize + UnsafeUtils.INT_SIZE * blockIds.length) {
    //   val (b1, b2) = blockIds.splitAt(blockIds.length / 2)
    //   val (c1, c2) = callbacks.splitAt(callbacks.length / 2)
    //   val r1 = fetchBlocksByBlockIds(executorId, b1, resultBufferAllocator, c1, amRecvStartCb)
    //   val r2 = fetchBlocksByBlockIds(executorId, b2, resultBufferAllocator, c2, amRecvStartCb)
    //   return r1 ++ r2
    // }

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum).order(ByteOrder.nativeOrder())
    buffer.putInt(t)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, (callbacks, amRecvStartCb, request, resultBufferAllocator))

    buffer.rewind()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    logDebug(s"Perftest send tag ${t} time ${System.nanoTime()}")
    ep.sendAmNonBlocking(UcpSparkAmId.FetchBlockReq, address, headerSize, dataAddress, buffer.capacity() - headerSize,
    UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
      override def onSuccess(request: UcpRequest): Unit = {
        buffer.clear()
         logDebug(s"Sent message on $ep to $executorId to fetch ${blockIds.length} blocks on tag $t" +
           s"in ${System.nanoTime() - startTime} ns")
      }
    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    // Seq(request)
  }

  @inline
  def initExecuter(executorId: transport.ExecutorId, nvkvHandler: NvkvHandler,
                   resultBufferAllocator: transport.BufferAllocator,
                   callback: OperationCallback): Unit = {
    progressThread.submit(new Runnable {
      override def run = {
        doInitExecuter(executorId, nvkvHandler, resultBufferAllocator, callback)
      }
    })
  }

  def doInitExecuter(executorId: transport.ExecutorId, nvkvHandler: NvkvHandler,
                     resultBufferAllocator: transport.BufferAllocator,
                     callback: OperationCallback): Unit = {
    val startTime = System.nanoTime()
    val ep = getConnection(executorId)
    val t = tag.incrementAndGet()
    val length = nvkvHandler.pack.capacity()

    // val buffer = Platform.allocateDirectBuffer(length).order(ByteOrder.nativeOrder())
    val buffer = Platform.allocateDirectBuffer(length)
    buffer.put(nvkvHandler.pack)
    buffer.rewind()


    // val request = new UcxRequest(null, new UcxStats())
    // requestData.put(t, (Seq(callback), request, resultBufferAllocator))

    val address = UnsafeUtils.getAdress(buffer)
    logDebug(s"Sending message to init executer $executorId with length $length")

    ep.sendAmNonBlocking(UcpSparkAmId.InitExecutorReq, 0, 0, address, length,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         logDebug(s"Sent message on $ep to $executorId to ini executer" +
           s"in ${System.nanoTime() - startTime}ns")
       }

       override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send init executer message $errorMsg")
        }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    while (!connectToRemoteNvkv) {progress()}
    // request
  }

  @inline
  def commitBlock(executorId: transport.ExecutorId, nvkvHandler: NvkvHandler,
                  resultBufferAllocator: transport.BufferAllocator, packMapperData: ByteBuffer,
                  callback: OperationCallback): Unit = {
    progressThread.submit(new Runnable {
      override def run = {
        doCommitBlock(executorId, nvkvHandler, resultBufferAllocator, packMapperData, callback)
      }
    })
  }

  def doCommitBlock(executorId: transport.ExecutorId, nvkvHandler: NvkvHandler,
                    resultBufferAllocator: transport.BufferAllocator, packMapperData: ByteBuffer,
                    callback: OperationCallback): Unit = {
    val startTime = System.nanoTime()
    val ep = getConnection(executorId)
    val t = tag.incrementAndGet()
    val length = packMapperData.capacity()

    val buffer = Platform.allocateDirectBuffer(length)
    buffer.put(packMapperData)
    buffer.rewind()


    // val request = new UcxRequest(null, new UcxStats())
    // requestData.put(t, (Seq(callback), request, resultBufferAllocator))

    val address = UnsafeUtils.getAdress(buffer)
    logDebug(s"Sending message to commit mapper info with length $length")

    ep.sendAmNonBlocking(UcpSparkAmId.MapperInfo, 0, 0, address, length,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         logDebug(s"Sent message on $ep to $executorId to ini executer" +
           s"in ${System.nanoTime() - startTime}ns")
       }

       override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send init executer message $errorMsg")
        }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    // request
  }

  // def handleFetchBlockRequest(blocks: Seq[Block], replyTag: Int, replyExecutor: Long): Unit = try {
  //   val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.length
  //   val resultMemory = transport.hostBounceBufferMemoryPool.get(tagAndSizes + blocks.map(_.getSize).sum)
  //     .asInstanceOf[UcxBounceBufferMemoryBlock]
  //   val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
  //     resultMemory.size)
  //   resultBuffer.putInt(replyTag)

  //   var offset = 0
  //   val localBuffers = blocks.zipWithIndex.map {
  //     case (block, i) =>
  //       resultBuffer.putInt(UnsafeUtils.INT_SIZE + i * UnsafeUtils.INT_SIZE, block.getSize.toInt)
  //       resultBuffer.position(tagAndSizes + offset)
  //       val localBuffer = resultBuffer.slice()
  //       offset += block.getSize.toInt
  //       localBuffer.limit(block.getSize.toInt)
  //       localBuffer
  //   }
  //   // Do parallel read of blocks
  //   val blocksCollection = if (transport.ucxShuffleConf.numIoThreads > 1) {
  //     val parCollection = blocks.indices.par
  //     parCollection.tasksupport = ioTaskSupport
  //     parCollection
  //   } else {
  //     blocks.indices
  //   }

  //   for (i <- blocksCollection) {
  //     blocks(i).getBlock(localBuffers(i))
  //   }

  //   val startTime = System.nanoTime()
  //   val req = getConnection(replyExecutor).sendAmNonBlocking(0 , resultMemory.address, tagAndSizes,
  //     resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes, 0, new UcxCallback {
  //       override def onSuccess(request: UcpRequest): Unit = {
  //         logTrace(s"Sent ${blocks.length} blocks of size: ${resultMemory.size} " +
  //           s"to tag $replyTag in ${System.nanoTime() - startTime} ns.")
  //         transport.hostBounceBufferMemoryPool.put(resultMemory)
  //       }

  //       override def onError(ucsStatus: Int, errorMsg: String): Unit = {
  //         logError(s"Failed to send $errorMsg")
  //       }
  //     }, new UcpRequestParams().setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
  //       .setMemoryHandle(resultMemory.memory))
  // } catch {
  //   case ex: Throwable => logError(s"Failed to read and send data: $ex")
  // }
}
