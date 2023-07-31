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
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.internal.Logging
import java.nio.BufferOverflowException
import org.openucx.jnvkv.NvkvException

object NvkvHandler {
  private var worker: NvkvHandler = null

  def getHandler(ucxContext: UcpContext, numOfPartitions: Int): NvkvHandler = {
    if (worker == null) worker = new NvkvHandler(ucxContext, numOfPartitions)
    worker
  }
}

class NvkvHandler private(ucxContext: UcpContext, private var numOfPartitions: Long) extends Logging {
  final private val nvkvBufferSize = 41943040
  // TODO - read in chunks
  final private val nvkvReadBufferSize = 41943040
  final private val nvkvRemoteReadBufferSize: Long = 1024*1024*43
  final private val nvkvNumOfReadBuffers: Long = 16
  final private val alignment = 512
  private var nvkvWriteBuffer: ByteBuffer = null
  private var nvkvReadBuffer: ByteBuffer = null
  private val nvkvReadBufferMpoolSize: Int = 64
  private var readBufferMpool: ListBuffer[ByteBuffer] = ListBuffer()
  private var nvkvRemoteReadBuffer: ByteBuffer = null
  private var nvkv: Nvkv.Context = null
  private var ds_idx = 0
  private var nvkvSize = 0L
  private var partitionSize = 0L
  private var packData: ByteBuffer = null
  val numOfMappers  = SparkEnv.get.conf.getInt("spark.groupByTest.numMappers", 1)
  val numOfReducers = SparkEnv.get.conf.getInt("spark.groupByTest.numReducers", 1)
  private var reducePartitions: Array[Array[ReducePartition]] = Array.ofDim[ReducePartition](numOfMappers, numOfReducers)
  val pciAddress = "0000:a1:00.0"
  val logEnabled = SparkEnv.get.conf.getInt("spark.nvkvLogs.enabled", 0)

  private def nvkvLogDebug(msg: => String) {
    if (logEnabled == 1) {
      logDebug(msg)
    }
  }

  nvkvLogDebug(s"LEO NvkvHandler constructor")
  Nvkv.init("mlx5_0", logEnabled, "0x1")
  val ds: Array[Nvkv.DataSet] = Nvkv.query()
  var detected = false

  if (ds.length == 0) throw new IllegalArgumentException("Can't detect nvkv device!")

  this.nvkv = Nvkv.open(ds, Nvkv.LOCAL|Nvkv.REMOTE)
  this.nvkvWriteBuffer = nvkv.alloc(nvkvBufferSize)
  // this.nvkvReadBuffer = nvkv.alloc(nvkvReadBufferSize)
  this.nvkvRemoteReadBuffer = nvkv.alloc(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
  this.nvkvSize = ds(0).size
  this.partitionSize = this.nvkvSize / numOfPartitions

  var nvkvCtx: Array[Byte] = ByteBuffer.wrap(this.nvkv.export()).order(ByteOrder.nativeOrder()).array()
  var nvkvCtxSize: Int = nvkvCtx.length

  nvkvLogDebug(s"LEO Register bb")
  val mem: UcpMemory = ucxContext.registerMemory(this.nvkvRemoteReadBuffer)
  var mkeyBuffer: ByteBuffer = null
  mkeyBuffer = mem.getExportedMkeyBuffer()
  
  nvkvLogDebug(s"LEO Try to pack nvkv")
  // nvkvCtx size + nvkvCtx + readBuf + readBuf length + max block size + mkeyBuffer size + mkeyBuffer
  packData = ByteBuffer.allocateDirect(UnsafeUtils.INT_SIZE +  // nvkvCtx size
                                       nvkvCtxSize +           // nvkvCtx
                                       UnsafeUtils.LONG_SIZE + // readBuf Address
                                       UnsafeUtils.LONG_SIZE + // readBuf length
                                       UnsafeUtils.INT_SIZE +  // max block size
                                       UnsafeUtils.INT_SIZE +  // readBuffer mkey size
                                       mkeyBuffer.capacity()).order(ByteOrder.nativeOrder())
  packData.putInt(nvkvCtxSize)
  packData.put(nvkvCtx)
  packData.putLong(UnsafeUtils.getAdress(this.nvkvRemoteReadBuffer))
  packData.putLong(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
  packData.putInt(nvkvRemoteReadBufferSize.toInt)
  packData.putInt(mkeyBuffer.capacity())
  packData.put(mkeyBuffer)
  packData.rewind()

  nvkvLogDebug(s"LEO packedNvkv nvkvCtx ${nvkvCtx} nvkvCtxSize ${nvkvCtxSize} bb ${UnsafeUtils.getAdress(this.nvkvRemoteReadBuffer)}")
  nvkvLogDebug(s"LEO packedNvkv packData capacity ${mkeyBuffer.capacity()} packData limit ${mkeyBuffer.limit()}")

  def getReadBuffer(length: Int): ByteBuffer = {
    var res_bb: ByteBuffer = null
    if (nvkvReadBuffer == null) {
      val bbSize = (length * 1.2).toInt
      nvkvReadBuffer = nvkv.alloc(nvkvReadBufferMpoolSize * bbSize)

      var pos = 0
      while (pos < nvkvReadBuffer.capacity()) {
        var bb: ByteBuffer = nvkvReadBuffer.slice()
        bb.limit(bbSize)
        pos += bbSize
        nvkvReadBuffer.position(pos)
      }

      nvkvReadBuffer.position(0)
    }

    if (readBufferMpool.length > 0) {
      res_bb = readBufferMpool.remove(0)
    }

    res_bb
  }

  def pack: ByteBuffer = this.packData

  def connectToRemote(add: Array[Byte]): Unit = {
    try {
      nvkv.connect(add)
    } catch {
      case e: NvkvException => logDebug("LEO NvkvHandler: Failed to connect to remote nvkv received from DPU")
    }
  }
  
  protected class Request(private var buffer: ByteBuffer, private var length: Long, private var offset: Long) {
    private var complete = false
    def setComplete(): Unit = {
      this.complete = true
    }

    def getComplete(): Boolean = {
      this.complete
    }

    def getLength: Long = this.length
    def getOffset: Long = this.offset
    def getBuffer: ByteBuffer = buffer
  }

  class WriteRequest(private var dsIdx: Int, private var source: ByteBuffer, length: Long, offset: Long) extends Request(source, length, offset) {
    def getDsIdx: Int = dsIdx
  }

  class ReadRequest(private var dest: ByteBuffer, length: Long, offset: Long) extends Request(dest, length, offset) {
  }

  private class ReducePartition(private var offset: Long, private var length: Long) {
    def getOffset: Long = this.offset
    def getLength: Long = this.length
  }

  private def post(request: WriteRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post write")
    try nvkv.postWrite(request.getDsIdx, this.nvkvWriteBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  private def post(request: ReadRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post read")
    //TODO - read from the right ds
    try nvkv.postRead(this.ds_idx, request.getBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  def pollCompletion(request: Request): Unit = {
    while (!request.getComplete()) nvkv.progress
  }

  def getPartitionSize: Long = this.partitionSize
  def getNumOfDevices: Int = ds.length

  private def test(length: Int): Unit = {
    val nvkvWriteBufferTmp = nvkvWriteBuffer.duplicate
    nvkvWriteBufferTmp.limit(length)
    val nvkvReadBufferTmp = nvkvReadBuffer.duplicate
    nvkvReadBufferTmp.limit(length)
    if (!(nvkvWriteBufferTmp == nvkvReadBufferTmp)) throw new RuntimeException("Data is corrupted")
  }

  private def getAlignedLength(length: Int) = if (length % 512 == 0) {length} else {length + (alignment - (length % alignment))}

  def read(length: Int, offset: Long): ByteBuffer = {
    val alignedLength = getAlignedLength(length)
    nvkvLogDebug(s"LEO NvkvHandler read size aligned " + alignedLength)
    val readRequest = new ReadRequest(nvkvReadBuffer, alignedLength, offset)
    post(readRequest)
    pollCompletion(readRequest)
    nvkvLogDebug(s"LEO NvkvHandler read complete")

    var clone: ByteBuffer = ByteBuffer.allocateDirect(length)
    nvkvReadBuffer.limit(length)
    clone.put(nvkvReadBuffer)
    clone.rewind()
    clone.limit(length)
    nvkvReadBuffer.limit(nvkvReadBuffer.capacity())
    nvkvReadBuffer.rewind()
    clone
  }

  def write(dsIdx: Int, shuffleId: Int, mapId: Long, 
            reducePartitionId: Int, bytes: Array[Byte], length: Int, offset: Long): Unit = {
    val source: ByteBuffer = ByteBuffer.wrap(bytes)
    var relativeOffset: Long = offset - nvkvWriteBuffer.position()
    var sourceLimit: Int = 0
    var remain: Int = length

    while (remain > 0) {
        if (!nvkvWriteBuffer.hasRemaining()) {
            nvkvWriteBuffer.rewind()
            nvkvLogDebug(s"LEO NvkvHandler spill buffer relativeOffset $relativeOffset")
            val writeRequest = new WriteRequest(dsIdx, nvkvWriteBuffer, nvkvBufferSize, relativeOffset)
            post(writeRequest)
            pollCompletion(writeRequest)
            nvkvLogDebug(s"LEO NvkvHandler write complete")

            // read(nvkvBufferSize, relativeOffset)
            // test(nvkvBufferSize)

            relativeOffset += nvkvBufferSize
        } else {
            sourceLimit = (source.position()+nvkvWriteBuffer.remaining()).min(length)
            nvkvLogDebug(s"LEO source_position ${source.position()} buffer_remaining ${nvkvWriteBuffer.remaining()} source_capacity ${length}")
            nvkvLogDebug(s"LEO NvkvHandler write to buffer from offset ${source.position()} length ${sourceLimit - source.position()}")
            remain -= (sourceLimit - source.position());
            source.limit(sourceLimit)
            nvkvWriteBuffer.put(source)
        }
    }
  }

  def writeRemaining(dsIdx: Int, offset: Long): Int = {
    val bufferPosition = nvkvWriteBuffer.position()
    var relativeOffset: Long = offset - bufferPosition
    nvkvWriteBuffer.rewind()
    nvkvLogDebug(s"LEO NvkvHandler write remaining size ${bufferPosition} relativeOffset ${relativeOffset}")
    val writeRequest = new WriteRequest(dsIdx, nvkvWriteBuffer, getAlignedLength(bufferPosition), relativeOffset)
    post(writeRequest)
    pollCompletion(writeRequest)
    nvkvLogDebug(s"LEO NvkvHandler write complete")
    // read(bufferPosition, relativeOffset)
    // test(bufferPosition)
    (getAlignedLength(bufferPosition) - bufferPosition)
  }

  def commitPartition(dsIdx: Int, start: Long, length: Long, shuffleId: Int, 
                      mapId: Long, reducePartitionId: Int): Unit = {
    nvkvLogDebug(s"LEO NvkvHandler commitPartition $shuffleId,$mapId,$reducePartitionId offset $start length $length dsIdx $dsIdx")
    reducePartitions(mapId.toInt)(reducePartitionId) = new ReducePartition(start+dsIdx*nvkvSize, length)
  }

  def getPartitonOffset(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    nvkvLogDebug(s"LEO NvkvHandler getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (this.reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      this.reducePartitions(mapId.toInt)(reducePartitionId).getOffset
    }
  }
  def getPartitonLength(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    nvkvLogDebug(s"LEO NvkvHandler getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (this.reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      this.reducePartitions(mapId.toInt)(reducePartitionId).getLength
    }
  }
}
