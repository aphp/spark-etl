/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.frama.parisni.spark.postgres.rowconverters

import java.util

import de.bytefish.pgbulkinsert.row.SimpleRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


object PgBulkInsertConverter {

  def handleNullInArray[T, U](array: Seq[Any], box: (T => U)): util.Collection[U] = {
    array.map((v: Any) => {
      if (v == null) v else box(v.asInstanceOf[T])
    }).asJava.asInstanceOf[util.Collection[U]]
  }

  val pgBulkInsertBytea = de.bytefish.pgbulkinsert.pgsql.constants.DataType.Bytea
  val pgBulkInsertDate = de.bytefish.pgbulkinsert.pgsql.constants.DataType.Date
  val pgBulkInsertTimestamp = de.bytefish.pgbulkinsert.pgsql.constants.DataType.Timestamp

  def makePgBulkInsertRowConverter(t: DataType, idx: Int): ((Row, SimpleRow) => Unit) = {
    (r: Row, s: SimpleRow) => {
      // Write data only if it is not null
      if (!r.isNullAt(idx)) {
        t match {
          case BooleanType => s.setBoolean(idx, r.getBoolean(idx))
          // No tinyint in postgres, converting to smallint instead
          case ByteType => s.setShort(idx, r.getByte(idx).toShort)
          case ShortType => s.setShort(idx, r.getShort(idx))
          case IntegerType => s.setInteger(idx, r.getInt(idx))
          case LongType => s.setLong(idx, r.getLong(idx))
          case FloatType => s.setFloat(idx, r.getFloat(idx))
          case DoubleType => s.setDouble(idx, r.getDouble(idx))
          case DecimalType() => s.setNumeric(idx, r.getDecimal(idx))

          case StringType => s.setText(idx, r.getString(idx))
          case BinaryType => s.setByteArray(idx, r.get(idx).asInstanceOf[Array[Byte]])

          case DateType => s.setDate(idx, r.getDate(idx).toLocalDate)
          case TimestampType => s.setTimeStamp(idx, r.getTimestamp(idx).toLocalDateTime)
          // TODO: Check if we prefer the following in comparison to toLocalDate
          //case TimestampType => s.setTimeStampTz(idx, ZonedDateTime.ofInstant(r.getTimestamp(idx).toInstant, ZoneId.of("UTC")))

          case ArrayType(BooleanType, _) => s.setBooleanArray(idx, handleNullInArray(r.getSeq[Boolean](idx), Boolean.box))
          // // No tinyint in postgres, converting to smallint instead
          case ArrayType(ByteType, _) => s.setShortArray(idx, handleNullInArray(r.getSeq[Byte](idx), (b: Byte) => Short.box(b.toShort)))
          case ArrayType(ShortType, _) => s.setShortArray(idx, handleNullInArray(r.getSeq[Short](idx), Short.box))
          case ArrayType(IntegerType, _) => s.setIntegerArray(idx, handleNullInArray(r.getSeq[Int](idx), Int.box))
          case ArrayType(LongType, _) => s.setLongArray(idx, handleNullInArray(r.getSeq[Long](idx), Long.box))
          case ArrayType(FloatType, _) => s.setFloatArray(idx, handleNullInArray(r.getSeq[Float](idx), Float.box))
          case ArrayType(DoubleType, _) => s.setDoubleArray(idx, handleNullInArray(r.getSeq[Double](idx), Double.box))
          case ArrayType(DecimalType(), _) => s.setNumericArray(idx, handleNullInArray(r.getSeq[BigDecimal](idx), (v: BigDecimal) => v))
          // No need of specific null handling, String does it by default
          case ArrayType(StringType, _) => s.setTextArray(idx, r.getSeq[String](idx).asJava)
          // TODO: Use setCollection instead of predefined functions
          //case ArrayType(BinaryType, _) => s.setCollection(idx, pgBulkInsertBytea, r.get(idx).asInstanceOf[Array[Byte]])
          //case ArrayType(DateType, _) => s.setCollection(idx, pgBulkInsertDate, r.getDate(idx))
          //case ArrayType(TimestampType, _) => s.setCollection(idx, pgBulkInsertTimestamp, r.getTimestamp(idx))

          // TODO -- Add multidimensional arrays
          case ArrayType(_, _) => throw new UnsupportedOperationException("Trying to insert an Array not supported by PgBulkInsert")
          // TODO Convert to json (hstore is an extension) - Don't forget to add the schema change
          case MapType(_, _, _) => throw new UnsupportedOperationException("Trying to insert a spark Map into postgres - Not yet supported")
          case StructType(_) => throw new UnsupportedOperationException("Trying to insert a spark Struct into postgres - Not yet supported")

          // Not treated: CalendarIntervalType, ObjectType (JVM object), NullType (NULL values type), HiveStringType (internal), UserDefinedType (internal), AnyType
          case _ => throw new UnsupportedOperationException("Trying to insert a datatype not supported by PgBulkInsert")
        }
      }
    }
  }

  def makePgBulkInsertRowConverter(schema: StructType): ((Row, SimpleRow) => Unit) = {
    val converters = schema.fields.toSeq.zipWithIndex.map {case (sf: StructField, idx: Int) =>
      makePgBulkInsertRowConverter(sf.dataType, idx)
    }

    (r: Row, s: SimpleRow) => converters.foreach(c => c(r, s))

  }

}