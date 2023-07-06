package org.apache.spark.network.buffer

import io.netty.buffer.{ByteBufInputStream, Unpooled}
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

import java.io.IOException
import java.nio.ByteBuffer

class NvkvManagedBuffer(val buffer: ByteBuffer) extends ManagedBuffer {

  private var buf: ByteBuffer = buffer

  override def size: Long = buf.remaining

  @throws[IOException]
  override def nioByteBuffer: ByteBuffer = buf.duplicate

  @throws[IOException]
  override def createInputStream = new ByteBufInputStream(Unpooled.wrappedBuffer(buf))

  override def retain: ManagedBuffer = this

  override def release: ManagedBuffer = this

  @throws[IOException]
  override def convertToNetty: AnyRef = Unpooled.wrappedBuffer(buf)

  override def toString: String = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("buf", buf).toString
}
