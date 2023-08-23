package org.apache.spark.shuffle.ucx

import java.util.zip.CRC32
import scala.collection.mutable.ListBuffer
import org.openucx.jnvkv._
import org.openucx.jucx.ucp._
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucs.UcsConstants
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.{UnsafeUtils, CommonUtils}
import org.apache.spark.internal.Logging
import java.nio.BufferOverflowException
import org.openucx.jnvkv.NvkvException
import org.apache.spark.SparkException

object NvkvWrapper {
  private var nvkv: NvkvWrapper = null

  def getNvkv(ucxContext: UcpContext, numOfPartitions: Int, executerLocalId: Int): NvkvWrapper = {
    if (nvkv == null) nvkv = new NvkvWrapper(ucxContext, numOfPartitions, executerLocalId)
    nvkv
  }
}

class NvkvWrapper private(ucxContext: UcpContext, private var numOfPartitions: Int, executerLocalId: Int) extends Logging {
  class NvkvWrapperException(message: String, cause: Throwable)
    extends SparkException(message, cause) {

    def this(message: String) = this(message, null)
  }
  
  class NvkvWrapperExceptionIllegalOffset(offset: Long)
    extends NvkvWrapperException(s"Illegal offset $offset, must be aligned to $getAlignment")
  class NvkvWrapperExceptionIllegalLength(length: Long)
    extends NvkvWrapperException(s"Illegal length $length, must be aligned to $getAlignment")

  final private val nvkvWriteBufferSize  = 1024*1024*40L
  final private val nvkvReadBufferSize  = 1024*1024*40L
  final private val nvkvRemoteReadBufferSize = 1024*1024*43L
  final private val nvkvNumOfReadBuffers = 16
  final private val alignment = 512
  private var nvkvWriteBuffer: ByteBuffer = null
  private var nvkvReadBuffer: ByteBuffer = null
  private var nvkvRemoteReadBuffer: ByteBuffer = null
  private var nvkv: Nvkv.Context = null
  var ds: Array[Nvkv.DataSet] = Array()
  private var ds_idx = 0
  private var nvkvStorageSize = 0L
  private var partitionSize = 0L
  private var packData: ByteBuffer = null
  val numOfMappers  = SparkEnv.get.conf.getInt("spark.groupByTest.numMappers", 1)
  val numOfReducers = SparkEnv.get.conf.getInt("spark.groupByTest.numReducers", 1)
  private var reducePartitions: Array[Array[ReducePartition]] = Array.ofDim[ReducePartition](numOfMappers, numOfReducers)
  val nvkvLogEnabled = SparkEnv.get.conf.getInt("spark.nvkvLogs.enabled", 0)
  val blockManager = SparkEnv.get.blockManager.blockManagerId
  val executerId = blockManager.executorId.toInt
  val executerNvkvDeviceIndex = executerLocalId % getNvkvTopology.length
  val executerNvkvCoreMask = getCoreMask((executerLocalId / getNvkvTopology.length),
                                          getNvkvTopology(executerNvkvDeviceIndex))
  /* 
   * SPDK proccess core_mask has to be unique.
   * This is a temporary solution.
   * Ideal solution will use RPC mutex and will take into account NUMA topology.
   * See SparkDPU documentation for more information. 
   */
  var core_mask = (1 << (executerId-1)).toHexString

  private def verbosedNvkvLogDebug(msg: => String) {
    if (nvkvLogEnabled == 1) {
      logDebug(msg)
    }
  }

  logDebug(s"NvkvWrapper constructor executerLocalId $executerLocalId cpu_mask $executerNvkvCoreMask")
  try {
    Nvkv.init("mlx5_0", nvkvLogEnabled, executerNvkvCoreMask)
    logDebug(s"NvkvWrapper: Pass nvkv init")
    ds = Nvkv.query()
    if (ds.length == 0) {
      throw new NvkvException("Failed to detect nvme device!")
    }
    logDebug(s"NvkvWrapper: Successfully query nvkv devices")
    nvkv = Nvkv.open(ds, Nvkv.LOCAL|Nvkv.REMOTE)
    logDebug(s"NvkvWrapper: Pass nvkv open")
    nvkvWriteBuffer = nvkv.alloc(nvkvWriteBufferSize)
    logDebug(s"NvkvWrapper: Allocated write buffer")
    nvkvRemoteReadBuffer = nvkv.alloc(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
    logDebug(s"NvkvWrapper: Allocated BB read buffers")
  } catch {
    case e: NvkvException => throw new NvkvWrapperException("Failed to init", e)
    case e: Throwable => throw new NvkvWrapperException("Unexpected error occurred", e)
  }

  nvkvStorageSize = ds(0).size
  partitionSize = nvkvStorageSize / numOfPartitions

  var nvkvCtx: Array[Byte] = ByteBuffer.wrap(nvkv.export()).order(ByteOrder.LITTLE_ENDIAN).array()
  var nvkvCtxSize: Int = nvkvCtx.length

  verbosedNvkvLogDebug(s"NvkvWrapper: Register bb")
  val mem: UcpMemory = ucxContext.registerMemory(nvkvRemoteReadBuffer)
  var mkeyBuffer: ByteBuffer = null
  mkeyBuffer = mem.getExportedMkeyBuffer()
  
  verbosedNvkvLogDebug(s"NvkvWrapper: Try to pack nvkv")
  packData = ByteBuffer.allocateDirect(UnsafeUtils.INT_SIZE  + // nvkvCtx size
                                       nvkvCtxSize           + // nvkvCtx
                                       UnsafeUtils.LONG_SIZE + // readBuf Address
                                       UnsafeUtils.LONG_SIZE + // readBuf length
                                       UnsafeUtils.INT_SIZE  + // max block size
                                       UnsafeUtils.INT_SIZE  + // readBuffer mkey size
                                       mkeyBuffer.capacity()).order(ByteOrder.LITTLE_ENDIAN)
  packData.putInt(nvkvCtxSize)
  packData.put(nvkvCtx)
  packData.putLong(UnsafeUtils.getAdress(nvkvRemoteReadBuffer))
  packData.putLong(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
  packData.putInt(nvkvRemoteReadBufferSize.toInt)
  packData.putInt(mkeyBuffer.capacity())
  packData.put(mkeyBuffer)
  packData.rewind()

  verbosedNvkvLogDebug(s"NvkvWrapper: packedNvkv nvkvCtx ${nvkvCtx} nvkvCtxSize ${nvkvCtxSize} bb ${UnsafeUtils.getAdress(nvkvRemoteReadBuffer)}")
  verbosedNvkvLogDebug(s"NvkvWrapper: packedNvkv packData capacity ${mkeyBuffer.capacity()} packData limit ${mkeyBuffer.limit()}")
  logDebug(s"NvkvWrapper: Finish init NVKV")

  // TODO - store bb in array not ListBuffer
  // To be used when fetching local blocks using DPU - no need to transfer data just bb idx
  def createMPFromBuffer(buffer: ByteBuffer, bbSize: Int): ListBuffer[ByteBuffer] = {
    var mpool: ListBuffer[ByteBuffer] = ListBuffer()

    var pos = 0
    while (pos < buffer.capacity()) {
      var bb: ByteBuffer = buffer.slice()
      bb.limit(bbSize)
      mpool.append(bb)
      pos += bbSize
      buffer.position(pos)
    }

    buffer.position(0)
    mpool
  }
  
  def getCoreMask(cpuIdx: Int, numaCpus: Long): String = {
    val rightMost1 = numaCpus & -numaCpus;
    (rightMost1 << cpuIdx).toHexString
  }

  //TODO: Create a function for detecting storage devices NUMA topology
  private def getNvkvTopology: Array[Long] = Array(0x00000000FFFFFFFFL, 0xFFFFFFFF00000000L)

  def pack: ByteBuffer = packData

  def connectToRemote(add: Array[Byte]): Unit = {
    try {
      nvkv.connect(add)
    } catch {
      case e: NvkvException => throw new NvkvWrapperException("Failed to connect to remote nvkv", e)
      case e: Throwable => throw new NvkvWrapperException("Unexpected error occurred", e)
    }
  }
  
  protected class Request(private var buffer: ByteBuffer, private var length: Long, private var offset: Long) {
    private var complete = false
    def setComplete(): Unit = {
      complete = true
    }

    def getComplete(): Boolean = {
      complete
    }

    def getLength: Long = length
    def getOffset: Long = offset
    def getBuffer: ByteBuffer = buffer
  }

  class WriteRequest(private var dsIdx: Int, private var source: ByteBuffer, length: Long, offset: Long) extends Request(source, length, offset) {
    def getDsIdx: Int = dsIdx
  }

  class ReadRequest(private var dest: ByteBuffer, length: Long, offset: Long) extends Request(dest, length, offset) {
  }

  private class ReducePartition(private var offset: Long, private var length: Long) {
    def getOffset: Long = offset
    def getLength: Long = length
  }

  private def checkRequest(request: Request) {
    if (request.getOffset % getAlignment != 0) {
      throw new NvkvWrapperExceptionIllegalOffset(request.getOffset)
    }
    
    if (request.getLength % getAlignment != 0) {
      throw new NvkvWrapperExceptionIllegalLength(request.getLength)
    }
  }

  private def post(request: WriteRequest) = {
    verbosedNvkvLogDebug(s"NvkvWrapper post write")
    checkRequest(request)

    try nvkv.postWrite(request.getDsIdx, nvkvWriteBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        verbosedNvkvLogDebug(s"NvkvWrapper post completed!")
      }
    })
    catch {
      case e: NvkvException => throw new NvkvWrapperException("Failed to post write request", e)
      case e: Throwable => throw new NvkvWrapperException("Unexpected error occurred", e)
    }
  }

  private def post(request: ReadRequest) = {
    verbosedNvkvLogDebug(s"NvkvWrapper post read")
    checkRequest(request)

    try nvkv.postRead(ds_idx, request.getBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        verbosedNvkvLogDebug(s"NvkvWrapper post completed!")
      }
    })
    catch {
      case e: NvkvException => throw new NvkvWrapperException("Failed to post read request", e)
      case e: Throwable => throw new NvkvWrapperException("Unexpected error occurred", e)
    }
  }

  def pollCompletion(request: Request): Unit = {
    CommonUtils.safePolling(() => {nvkv.progress}, () => {!request.getComplete()})
  }

  def getPartitionSize: Long = partitionSize
  def getNumOfDevices: Int = ds.length

  private def test(length: Int): Unit = {
    val nvkvWriteBufferTmp = nvkvWriteBuffer.duplicate
    nvkvWriteBufferTmp.limit(length)
    val nvkvReadBufferTmp = nvkvReadBuffer.duplicate
    nvkvReadBufferTmp.limit(length)
    if (!(nvkvWriteBufferTmp == nvkvReadBufferTmp)) {
      throw new NvkvWrapperException("Data is corrupted")
    }
  }

  private def getAlignedLength(length: Int) = if (length % getAlignment == 0) {length} else {length + (alignment - (length % alignment))}

  def getAlignment: Int = alignment

  def read(length: Int, offset: Long): ByteBuffer = {
    val alignedLength = getAlignedLength(length)
    verbosedNvkvLogDebug(s"NvkvWrapper read aligned size " + alignedLength)
    val readRequest = new ReadRequest(nvkvReadBuffer, alignedLength, offset)
    post(readRequest)
    pollCompletion(readRequest)
    verbosedNvkvLogDebug(s"NvkvWrapper read complete")

    var clone: ByteBuffer = ByteBuffer.allocateDirect(length)
    nvkvReadBuffer.limit(length)
    clone.put(nvkvReadBuffer)
    clone.rewind()
    clone.limit(length)
    nvkvReadBuffer.limit(nvkvReadBuffer.capacity())
    nvkvReadBuffer.rewind()
    clone
  }

  def write(shuffleId: Int, mapId: Long, 
            reducePartitionId: Int, bytes: Array[Byte], length: Int, offset: Long): Unit = {
    val source: ByteBuffer = ByteBuffer.wrap(bytes)
    var relativeOffset: Long = offset - nvkvWriteBuffer.position()
    var sourceLimit: Int = 0
    var remain: Int = length

    while (remain > 0) {
        if (!nvkvWriteBuffer.hasRemaining()) {
            nvkvWriteBuffer.rewind()
            verbosedNvkvLogDebug(s"NvkvWrapper spill buffer relativeOffset $relativeOffset")
            val writeRequest = new WriteRequest(executerNvkvDeviceIndex, nvkvWriteBuffer, nvkvWriteBufferSize, relativeOffset)
            post(writeRequest)
            pollCompletion(writeRequest)
            verbosedNvkvLogDebug(s"NvkvWrapper write complete")
            relativeOffset += nvkvWriteBufferSize
        } else {
            sourceLimit = (source.position()+nvkvWriteBuffer.remaining()).min(length)
            verbosedNvkvLogDebug(s"source_position ${source.position()} buffer_remaining ${nvkvWriteBuffer.remaining()} source_capacity ${length}")
            verbosedNvkvLogDebug(s"NvkvWrapper write to buffer from offset ${source.position()} length ${sourceLimit - source.position()}")
            remain -= (sourceLimit - source.position());
            source.limit(sourceLimit)
            nvkvWriteBuffer.put(source)
        }
    }
  }

  def writeRemaining(offset: Long): Int = {
    val bufferPosition = nvkvWriteBuffer.position()
    var relativeOffset: Long = offset - bufferPosition
    nvkvWriteBuffer.rewind()
    verbosedNvkvLogDebug(s"NvkvWrapper write remaining size ${bufferPosition} relativeOffset ${relativeOffset}")
    val writeRequest = new WriteRequest(executerNvkvDeviceIndex, nvkvWriteBuffer, getAlignedLength(bufferPosition), relativeOffset)
    post(writeRequest)
    pollCompletion(writeRequest)
    verbosedNvkvLogDebug(s"NvkvWrapper write complete")
    (getAlignedLength(bufferPosition) - bufferPosition)
  }

  def commitPartition(start: Long, length: Long, shuffleId: Int, 
                      mapId: Long, reducePartitionId: Int): Unit = {
    verbosedNvkvLogDebug(s"NvkvWrapper commitPartition $shuffleId,$mapId,$reducePartitionId offset $start length $length executerNvkvDeviceIndex $executerNvkvDeviceIndex")
    reducePartitions(mapId.toInt)(reducePartitionId) = new ReducePartition(start+executerNvkvDeviceIndex*nvkvStorageSize, length)
  }

  def getPartitonOffset(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    verbosedNvkvLogDebug(s"NvkvWrapper getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      reducePartitions(mapId.toInt)(reducePartitionId).getOffset
    }
  }

  def getPartitonLength(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    verbosedNvkvLogDebug(s"NvkvWrapper getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      reducePartitions(mapId.toInt)(reducePartitionId).getLength
    }
  }
}
