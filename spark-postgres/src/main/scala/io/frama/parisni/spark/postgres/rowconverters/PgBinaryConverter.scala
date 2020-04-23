package io.frama.parisni.spark.postgres.rowconverters

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.function

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Row

class PgBinaryConverter(
  rowWriter: Map[Int, ((Row, DataOutputStream) => Unit)],
  bufferSize: Int = 64 * 1024
) extends LazyLogging {

  private val buffer: ByteArrayOutputStream = new ByteArrayOutputStream(bufferSize)
  private val outputStream: DataOutputStream = new DataOutputStream(buffer)
  private val columnNumber = this.rowWriter.size

  var nullCharacterHandler: function.Function[String, String] = new java.util.function.Function[String, String] {
    override def apply(t: String): String = t
  }

  def convertRow(row: Row): Array[Byte] = {
    buffer.reset()
    outputStream.writeShort(columnNumber)
    (0 until columnNumber).foreach(idx => rowWriter(idx)(row, outputStream))
    buffer.toByteArray
  }

  def close(): Unit = {
    outputStream.close()
    buffer.close()
  }

}