package io.frama.parisni.spark.postgres.rowconverters

import java.io.DataOutputStream

import org.apache.spark.sql.Row

class PgBinaryWriter(
    outputStream: DataOutputStream,
    rowWriter: Map[Int, ((Row, DataOutputStream) => Unit)]
) {

  private val columnNumber = this.rowWriter.size

  def writeRow(row: Row): Unit = {
    outputStream.writeShort(columnNumber)
    (0 until columnNumber).foreach(idx => rowWriter(idx)(row, outputStream))
  }

}
