package io.frama.parisni.spark.postgres

import java.sql.SQLException
import java.util.function
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider
import de.bytefish.pgbulkinsert.row.SimpleRow
import de.bytefish.pgbulkinsert.util.{StringUtils, PostgreSqlUtils}
import org.postgresql.PGConnection
import org.postgresql.copy.PGCopyOutputStream

import scala.collection.JavaConverters._


class PgBulkInserter(
  table: String,
  columns: Seq[String],
  usePostgresQuoting: Boolean,
  bufferSize: Int = 64 * 1024
) extends LazyLogging {

  @transient
  val writer: PgBinaryWriter = new PgBinaryWriter(bufferSize)
  val provider: ValueHandlerProvider = new ValueHandlerProvider()
  val lookup: Map[String, Int] = columns.zipWithIndex.toMap

  var nullCharacterHandler: function.Function[String, String] = new java.util.function.Function[String, String] {
    override def apply(t: String): String = t
  }
  var isOpened: Boolean = false
  var isClosed: Boolean = false

  def getCopyCommand: String = {
    val fullyQualifiedTableName = PostgreSqlUtils.getFullyQualifiedTableName(null, table, usePostgresQuoting)
    val commaSeparatedColumns = columns.map(c => if (usePostgresQuoting) PostgreSqlUtils.quoteIdentifier(c) else c).mkString(", ")
    s"COPY $fullyQualifiedTableName($commaSeparatedColumns) FROM STDIN BINARY"
  }

  @throws(classOf[SQLException])
  def open(connection: PGConnection) {
    writer.open(new PGCopyOutputStream(connection, getCopyCommand, bufferSize))
    isClosed = false
    isOpened = true
  }



  def startRow(consumer: Consumer[SimpleRow]) {
    if (!isOpened) {
      throw new BinaryWriteFailedException("The SimpleRowWriter has not been opened")
    }
    if (isClosed) {
      throw new BinaryWriteFailedException("The PGCopyOutputStream has already been closed")
    }
    try {
      writer.startRow(this.columns.length)
      val row: SimpleRow = new SimpleRow(provider, lookup.map{ case(k, v) => k -> Int.box(v)}.asJava, this.nullCharacterHandler)
      consumer.accept(row)
      row.writeRow(writer)
    }
    catch {
      case e: Exception =>
        try {
          logger.error("Problem writing row with PgBulkInsert writer, closing")
          close()
        }
        catch {
          case ex: Exception =>
            logger.error("Problem closing the PgBulkInsert writer")
        }
        throw e
    }
  }

  def close(): Unit = {
    isOpened = false
    isClosed = true
    writer.close()
  }

  def setNullCharacterHandler(nullCharacterHandlerParam: (String) => String) = {
    this.nullCharacterHandler = new java.util.function.Function[String, String] {
      override def apply(t: String): String = nullCharacterHandlerParam(t)
    }
  }

  def enableNullCharacterHandler(): Unit = this.setNullCharacterHandler(StringUtils.removeNullCharacter)

}