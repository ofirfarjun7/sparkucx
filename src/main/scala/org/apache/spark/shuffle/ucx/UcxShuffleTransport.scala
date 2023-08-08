/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils, DpuUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class UcxRequest(private var request: UcpRequest, stats: OperationStats)
  extends Request {

  private[ucx] var completed = false

  override def isCompleted: Boolean = completed || ((request != null) && request.isCompleted)

  override def getStats: Option[OperationStats] = Some(stats)

  private[ucx] def setRequest(request: UcpRequest): Unit = {
    this.request = request
  }
}

class UcxStats extends OperationStats {
  private[ucx] val startTime = System.nanoTime()
  private[ucx] var amHandleTime = 0L
  private[ucx] var endTime: Long = 0L
  private[ucx] var receiveSize: Long = 0L

  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  override def getElapsedTimeNs: Long = endTime - startTime

  /**
   * Indicates number of valid bytes in receive memory
   */
  override def recvSize: Long = receiveSize
}

case class UcxShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def serializedSize: Int = 12

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(shuffleId)
    byteBuffer.putInt(mapId)
    byteBuffer.putInt(reduceId)
  }
}

object UcxShuffleBlockId {
  def deserialize(byteBuffer: ByteBuffer): UcxShuffleBlockId = {
    val shuffleId = byteBuffer.getInt
    val mapId = byteBuffer.getInt
    val reduceId = byteBuffer.getInt
    UcxShuffleBlockId(0, mapId, reduceId)
  }
}

class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: Long = 0) extends ShuffleTransport
  with Logging {
  @volatile private var initialized: Boolean = false
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  // private var listener: UcpListener = _
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  val endpoints = mutable.Set.empty[UcpEndpoint]
  val executorAddresses = new TrieMap[ExecutorId, ByteBuffer]
  var nvkvHandler: NvkvHandler = null

  private var allocatedClientWorkers: Array[UcxWorkerWrapper] = _

  //TODO - Check why Peter chose TrieMap to store the reduce blocks info
  private val registeredBlocks = new TrieMap[BlockId, Block]
  private var progressThread: Thread = _
  var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _
  private var localDpuEp: UcpEndpoint = null

  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint)
      ucpEndpoint.close()
    }
  }

  def connectToLocalDpu(): Unit = {
    val address = new InetSocketAddress(DpuUtils.getLocalDpuAddress, 1338)
    logDebug(s"LEO Connecting to local DPU at $address")

    val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setSocketAddress(address).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to local DPU $address got an error: $errorMsg")
          }
        }).setName(s"Endpoint to local DPU $address")

    localDpuEp = globalWorker.newEndpoint(endpointParams)
  }

  def getNvkvHandler: NvkvHandler = nvkvHandler

  override def init(): Unit = {
    logDebug("LEO init UcxShuffleTransport")
    if (ucxShuffleConf == null) {
      ucxShuffleConf = new UcxShuffleConf(SparkEnv.get.conf)
    }

    // EP per 
    val numEndpoints = ucxShuffleConf.getSparkConf.getInt("spark.executor.instances", 1) * ucxShuffleConf.numWorkers
    
    // *
      //ucxShuffleConf.getSparkConf.getInt("spark.executor.instances", 1)// *
      //ucxShuffleConf.numListenerThreads // Each listener thread creates backward endpoint
    logInfo(s"Creating UCX context with an estimated number of endpoints: $numEndpoints")

    val params = new UcpParams().requestAmFeature().setMtWorkersShared(true).setEstimatedNumEps(numEndpoints)
      .requestAmFeature().setConfig("USE_MT_MUTEX", "yes").requestExportedMemFeature()

    if (ucxShuffleConf.useWakeup) {
      params.requestWakeupFeature()
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    ucxContext = new UcpContext(params)
    globalWorker = ucxContext.newWorker(ucpWorkerParams)
    hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxShuffleConf, ucxContext)

    nvkvHandler = NvkvHandler.getHandler(ucxContext, 1)

    // progressThread = new GlobalWorkerRpcThread(globalWorker, this)
    // progressThread.start()

    allocatedClientWorkers = new Array[UcxWorkerWrapper](ucxShuffleConf.numWorkers)
    logInfo(s"Allocating ${ucxShuffleConf.numWorkers} client workers")
    for (i <- 0 until ucxShuffleConf.numWorkers) {
      val clientId: Long = ((i.toLong + 1L) << 32) | executorId
      ucpWorkerParams.setClientId(clientId)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedClientWorkers(i) = UcxWorkerWrapper(worker, this, clientId)
      allocatedClientWorkers(i).start
    }

    initialized = true
    logDebug("LEO init UcxShuffleTransport done")
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      endpoints.foreach(_.closeNonBlockingForce())
      endpoints.clear()

      hostBounceBufferMemoryPool.close()

      allocatedClientWorkers.foreach(_.close())

      if (progressThread != null) {
        progressThread.interrupt()
        progressThread.join(10)
      }

      if (globalWorker != null) {
        globalWorker.close()
        globalWorker = null
      }

      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
    }
  }

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  override def addExecutor(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
    logDebug("LEO adding executor " + executorId)
    executorAddresses.put(executorId, workerAddress)
    allocatedClientWorkers.foreach(_.addExecutor(executorId))
  }

  def addExecutors(executorIdsToAddress: Map[ExecutorId, SerializableDirectBuffer]): Unit = {
    executorIdsToAddress.foreach {
      case (executorId, address) => {
        executorAddresses.put(executorId, address.value)
        logDebug("LEO adding executor " + executorId + " address " + address.value)
      }
    }
    allocatedClientWorkers.foreach(_.addExecutors)
  }

  def preConnect(): Unit = {
  }

  /**
   * Remove executor from communications.
   */
  override def removeExecutor(executorId: Long): Unit = executorAddresses.remove(executorId)

  /**
   * Registers blocks using blockId on SERVER side.
   */
  override def register(blockId: BlockId, block: Block): Unit = {
    registeredBlocks.put(blockId, block)
  }

  /**
   * Change location of underlying blockId in memory
   */
  override def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback): Unit = {
    unregister(blockId)
    register(blockId, newBlock)
    callback.onComplete(new OperationResult {
      override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

      override def getError: TransportError = null

      override def getStats: Option[OperationStats] = None

      override def getData: MemoryBlock = newBlock.getMemoryBlock
    })

  }

  /**
   * Indicate that this blockId is not needed any more by an application.
   * Note: this is a blocking call. On return it's safe to free blocks memory.
   */
  override def unregister(blockId: BlockId): Unit = {
    registeredBlocks.remove(blockId)
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    registeredBlocks.keysIterator.foreach(bid =>
      if (bid.asInstanceOf[UcxShuffleBlockId].shuffleId == shuffleId) {
        registeredBlocks.remove(bid)
      }
    )
  }

  def unregisterAllBlocks(): Unit = {
    registeredBlocks.clear()
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(executorId: ExecutorId, blockIds: Seq[BlockId],
                                     resultBufferAllocator: BufferAllocator,
                                     callbacks: Seq[OperationCallback],
                                     amRecvStartCb: () => Unit): Unit = {
    allocatedClientWorkers((Thread.currentThread().getId % allocatedClientWorkers.length).toInt)
      .fetchBlocksByBlockIds(executorId, blockIds, resultBufferAllocator, callbacks, amRecvStartCb)
  }

  def initExecuter(executorId: ExecutorId, resultBufferAllocator: BufferAllocator): Unit = {
    allocatedClientWorkers((Thread.currentThread().getId % allocatedClientWorkers.length).toInt)
      .initExecuter(executorId, nvkvHandler, resultBufferAllocator, (result: OperationResult) => {logDebug("Init executer in UCX")})
  }

  def commitBlock(executorId: ExecutorId, resultBufferAllocator: BufferAllocator, 
                  packMapperData: ByteBuffer): Unit = {
    logDebug(s"LEO commitBlock threadId ${Thread.currentThread().getId}")
    allocatedClientWorkers((Thread.currentThread().getId % allocatedClientWorkers.length).toInt)
      .commitBlock(executorId, nvkvHandler, resultBufferAllocator, packMapperData, (result: OperationResult) => {logDebug("Init executer in UCX")})
  }

  // def connectServerWorkers(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
  //   executorAddresses.put(executorId, workerAddress)
  //   allocatedServerWorkers.foreach(w => w.connectByWorkerAddress(executorId, workerAddress))
  // }

  def handleFetchBlockRequest(replyTag: Int, amData: UcpAmData, replyExecutor: Long): Unit = {
    // logDebug(s"LEO transport handleFetchBlockRequest")
    // val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
    // val blockIds = mutable.ArrayBuffer.empty[BlockId]

    // // 1. Deserialize blockIds from header
    // while (buffer.remaining() > 0) {
    //   val blockId = UcxShuffleBockId.deserialize(buffer)
    //   if (!registeredBlocks.contains(blockId)) {
    //     throw new UcxException(s"$blockId is not registered")
    //   }
    //   blockIds += blockId
    // }

    // val blocks = blockIds.map(bid => registeredBlocks(bid))
    // amData.close()
    // allocatedServerWorkers((Thread.currentThread().getId % allocatedServerWorkers.length).toInt)
    //   .handleFetchBlockRequest(blocks, replyTag, replyExecutor)
  }
}
