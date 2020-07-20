package io.frama.parisni.spark.postgres.rowconverters

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.spark.sql.Row

class PgBinaryConverter(
    rowWriter: Map[Int, ((Row, DataOutputStream) => Unit)],
    bufferSize: Int = 64 * 1024
) {

  private val buffer: ByteArrayOutputStream = new ByteArrayOutputStream(
    bufferSize
  )
  private val outputStream: DataOutputStream = new DataOutputStream(buffer)
  private val pgBinaryWriter: PgBinaryWriter =
    new PgBinaryWriter(outputStream, rowWriter)

  def convertRow(row: Row): Array[Byte] = {
    buffer.reset()
    pgBinaryWriter.writeRow(row)
    buffer.toByteArray
  }

}
