package io.frama.parisni.spark.postgres.rowconverters

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Kept for historical and performance test reasons
  * Univocity is 1 order of magnitude faster (20x)
  */
object PGConverter {

  def makeConverter(dataType: DataType): (Row, Int) => String =
    dataType match {
      case StringType    => (r: Row, i: Int) => r.getString(i)
      case BooleanType   => (r: Row, i: Int) => r.getBoolean(i).toString
      case ByteType      => (r: Row, i: Int) => r.getByte(i).toString
      case ShortType     => (r: Row, i: Int) => r.getShort(i).toString
      case IntegerType   => (r: Row, i: Int) => r.getInt(i).toString
      case LongType      => (r: Row, i: Int) => r.getLong(i).toString
      case FloatType     => (r: Row, i: Int) => r.getFloat(i).toString
      case DoubleType    => (r: Row, i: Int) => r.getDouble(i).toString
      case DecimalType() => (r: Row, i: Int) => r.getDecimal(i).toString

      case DateType =>
        (r: Row, i: Int) => r.getAs[Date](i).toString

      case TimestampType => (r: Row, i: Int) => r.getAs[Timestamp](i).toString

      case BinaryType =>
        (r: Row, i: Int) =>
          new String(r.getAs[Array[Byte]](i), StandardCharsets.UTF_8)

      //case udt: UserDefinedType[_] => makeConverter(udt.sqlType)
      case _ => (row: Row, ordinal: Int) => row.get(ordinal).toString
    }

  def convertRow(
      row: Row,
      length: Int,
      delimiter: String,
      valueConverters: Array[(Row, Int) => String]
  ): Array[Byte] = {
    var i = 0
    val values = new Array[String](length)
    while (i < length) {
      if (!row.isNullAt(i)) {
        values(i) =
          convertValue(valueConverters(i).apply(row, i), delimiter.charAt(0))
      } else {
        values(i) = "NULL"
      }
      i += 1
    }
    (values.mkString(delimiter) + "\n").getBytes("UTF-8")
  }

  def convertValue(str: String, delimiter: Char): String = {
    str.flatMap {
      case '\\'        => "\\\\"
      case '\n'        => "\\n"
      case '\r'        => "\\r"
      case `delimiter` => s"\\$delimiter"
      case c if c == 0 => "" // If this char is an empty character, drop it.
      case c           => s"$c"
    }
  }
}
