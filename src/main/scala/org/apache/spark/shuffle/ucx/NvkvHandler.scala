package org.apache.spark.shuffle.ucx

import java.util.zip.CRC32
import org.openucx.jnvkv._
import org.openucx.jucx.ucp._
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucs.UcsConstants
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.internal.Logging
import java.nio.BufferOverflowException

object NvkvHandler {
  private var worker: NvkvHandler = null

  def getHandler(ucxContext: UcpContext, numOfPartitions: Int): NvkvHandler = {
    if (worker == null) worker = new NvkvHandler(ucxContext, numOfPartitions)
    worker
  }
}

class NvkvHandler private(ucxContext: UcpContext, private var numOfPartitions: Long) extends Logging {
  final private val nvkvBufferSize = 8192
  // TODO - read in chunks
  final private val nvkvReadBufferSize = 31457280
  final private val alignment = 512
  private var nvkvWriteBuffer: ByteBuffer = null
  private var nvkvReadBuffer: ByteBuffer = null
  private var nvkv: Nvkv.Context = null
  private var ds_idx = 0
  private var nvkvSize = 0L
  private var partitionSize = 0L
  private var packData: ByteBuffer = null
  //TODO - init accourding to the number of shuffle, map, reduce
  private var reducePartitions: Array[Array[ReducePartition]] = Array.ofDim[ReducePartition](8, 4)
  val pciAddress = "0000:41:00.0"
  val logEnabled = false

  private def nvkvLogDebug(msg: => String) {
    if (logEnabled) {
      logDebug(msg)
    }
  }

  nvkvLogDebug(s"LEO NvkvHandler constructor")
  Nvkv.init("mlx5_0", 1, "0x1")
  val ds: Array[Nvkv.DataSet] = Nvkv.query()
  var detected = false

  for (i <- 0 until ds.length) {
    nvkvLogDebug(s"LEO nvkv device: pciAddr " + ds(i).pciAddr + " nsid " + ds(i).nsid + " size " + ds(i).size)
    if (ds(i).pciAddr.equals(pciAddress)) {
      this.ds_idx = i
      this.nvkvSize = ds(i).size
      detected = true
    }
  }
  if (!detected) throw new IllegalArgumentException("Nvkv device at address " + pciAddress + " not exists!")

  this.nvkv = Nvkv.open(ds, Nvkv.LOCAL|Nvkv.REMOTE)
  this.nvkvWriteBuffer = nvkv.alloc(nvkvBufferSize)
  this.nvkvReadBuffer = nvkv.alloc(nvkvReadBufferSize)
  this.partitionSize = this.nvkvSize / numOfPartitions

  var nvkvCtx: Array[Byte] = this.nvkv.export()
  var nvkvCtxSize: Int = nvkvCtx.length

  nvkvLogDebug(s"LEO Register bb")
  val mem: UcpMemory = ucxContext.registerMemory(this.nvkvReadBuffer)
  var mkeyBuffer: ByteBuffer = null
  mkeyBuffer = mem.getExportedMkeyBuffer()
  
  nvkvLogDebug(s"LEO Try to pack nvkv")
  // nvkvCtx size + nvkvCtx + readBuf + readBuf length + max block size + mkeyBuffer size + mkeyBuffer
  packData = ByteBuffer.allocateDirect(4 + nvkvCtxSize + 8 + 8 + 4 + 4 + mkeyBuffer.capacity()).order(ByteOrder.nativeOrder())
  packData.putInt(nvkvCtxSize)
  packData.put(nvkvCtx)
  packData.putLong(UnsafeUtils.getAdress(this.nvkvReadBuffer))
  packData.putLong(1 * nvkvReadBufferSize)
  packData.putInt(nvkvReadBufferSize)
  packData.putInt(mkeyBuffer.capacity())
  packData.put(mkeyBuffer)
  packData.rewind()

  nvkvLogDebug(s"LEO packedNvkv nvkvCtx ${nvkvCtx} nvkvCtxSize ${nvkvCtxSize} bb ${UnsafeUtils.getAdress(this.nvkvReadBuffer)}")
  nvkvLogDebug(s"LEO packedNvkv packData capacity ${mkeyBuffer.capacity()} packData limit ${mkeyBuffer.limit()}")

  def pack: ByteBuffer = this.packData
  
  private class Request(private var length: Long, private var offset: Long) {
    def getLength: Long = this.length
    def getOffset: Long = this.offset
  }

  private class WriteRequest(private var data: ByteBuffer, length: Long, offset: Long) extends Request(length, offset) {
  }

  private class ReadRequest(private var recvBuffer: ByteBuffer, length: Long, offset: Long) extends Request(length, offset) {
  }

  private class ReducePartition(private var offset: Long, private var length: Long) {
    def getOffset: Long = this.offset
    def getLength: Long = this.length
  }

  private class Completion(private var request: Request) {
    private var complete = false

    this.complete = false

    def getComplete: Boolean = this.complete

    def setComplete(status: Boolean): Unit = {
      this.complete = status
    }

    def getOffset: Long = this.request.getOffset

    def getLength: Long = this.request.getLength
  }

  private class WriteCompletion(request: Request) extends Completion(request) {
  }

  private class ReadCompletion(request: Request) extends Completion(request) {
  }

  private def post(request: WriteRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post write")
    val completion = new WriteCompletion(request)
    try nvkv.postWrite(this.ds_idx, this.nvkvWriteBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        completion.setComplete(true)
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
    completion
  }

  private def post(request: ReadRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post read")
    val completion = new ReadCompletion(request)
    try nvkv.postRead(this.ds_idx, this.nvkvReadBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        completion.setComplete(true)
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
    completion
  }

  private def pollCompletion(completion: Completion): Unit = {
    while (!completion.getComplete) nvkv.progress
  }

  def getPartitionSize: Long = this.partitionSize

  private def test(length: Int): Unit = {
    val nvkvWriteBufferTmp = nvkvWriteBuffer.duplicate
    nvkvWriteBufferTmp.limit(length)
    val nvkvReadBufferTmp = nvkvReadBuffer.duplicate
    nvkvReadBufferTmp.limit(length)
    if (!(nvkvWriteBufferTmp == nvkvReadBufferTmp)) throw new RuntimeException("Data is corrupted")
  }

  private def getAlignedLength(length: Int) = length + (alignment - (length % alignment))

  def read(length: Int, offset: Long): ByteBuffer = {
    val alignedLength = getAlignedLength(length)
    nvkvLogDebug(s"LEO NvkvHandler read size aligned " + alignedLength)
    val readRequest = new ReadRequest(nvkvReadBuffer, alignedLength, offset)
    val completion = post(readRequest)
    pollCompletion(completion)
    nvkvLogDebug(s"LEO NvkvHandler read complete")
    nvkvReadBuffer.rewind()

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
            nvkvLogDebug(s"LEO NvkvHandler spill buffer relativeOffset $relativeOffset")
            val writeRequest = new WriteRequest(nvkvWriteBuffer, nvkvBufferSize, relativeOffset)
            val completion = post(writeRequest)
            pollCompletion(completion)
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

  def writeRemaining(offset: Long): Int = {
    val bufferPosition = nvkvWriteBuffer.position()
    var relativeOffset: Long = offset - bufferPosition
    nvkvWriteBuffer.rewind()
    nvkvLogDebug(s"LEO NvkvHandler write remaining size ${bufferPosition} relativeOffset ${relativeOffset}")
    val writeRequest = new WriteRequest(nvkvWriteBuffer, getAlignedLength(bufferPosition), relativeOffset)
    val completion = post(writeRequest)
    pollCompletion(completion)
    nvkvLogDebug(s"LEO NvkvHandler write complete")
    // read(bufferPosition, relativeOffset)
    // test(bufferPosition)
    (getAlignedLength(bufferPosition) - bufferPosition)
  }

  def commitPartition(start: Long, length: Long, shuffleId: Int, 
                      mapId: Long, reducePartitionId: Int): Unit = {
    nvkvLogDebug(s"LEO NvkvHandler commitPartition $shuffleId,$mapId,$reducePartitionId offset $start length $length")
    reducePartitions(mapId.toInt)(reducePartitionId) = new ReducePartition(start, length)
  }

  def getPartitonOffset(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = this.reducePartitions(mapId.toInt)(reducePartitionId).getOffset
  def getPartitonLength(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = this.reducePartitions(mapId.toInt)(reducePartitionId).getLength
}
