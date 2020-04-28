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

import java.io.DataOutputStream
import java.sql.{Timestamp, Date}
import java.util

import de.bytefish.pgbulkinsert.pgsql.constants.{DataType, ObjectIdentifier}
import de.bytefish.pgbulkinsert.pgsql.handlers.{CollectionValueHandler, ValueHandlerProvider}
import de.bytefish.pgbulkinsert.util.StringUtils
import org.apache.spark.sql.types.{DataType => _, _}
import org.apache.spark.sql.{Row, types}

import scala.collection.JavaConverters._


object PgBulkInsertConverter {

  val provider = new ValueHandlerProvider()
  
  def makeCollectionValueHandler[T, U <: util.Collection[T]](datatype: de.bytefish.pgbulkinsert.pgsql.constants.DataType): CollectionValueHandler[T, U] = {
    new CollectionValueHandler(ObjectIdentifier.mapFrom(datatype), provider.resolve(datatype))
  }

  def handleArray[T, U](array: Seq[Any], box: (T => U)): util.Collection[U] = {
    array.map((v: Any) => {
      if (v == null) v else box(v.asInstanceOf[T])
    }).asJava.asInstanceOf[util.Collection[U]]
  }

  def makeFieldWriter(t: types.DataType, idx: Int): ((Row, DataOutputStream) => Unit) = {
    (r: Row, buffer: DataOutputStream) => {
      if (r.isNullAt(idx)) {
        provider.resolve(DataType.Text).handle(buffer, null)
      } else {
        t match {
          case BooleanType => provider.resolve(DataType.Boolean).handle(buffer, r.getBoolean(idx))
          // No tinyint in postgres, converting to smallint instead
          case ByteType => provider.resolve(DataType.Int2).handle(buffer, r.getByte(idx).toShort)
          case ShortType => provider.resolve(DataType.Int2).handle(buffer, r.getShort(idx))
          case IntegerType => provider.resolve(DataType.Int4).handle(buffer, r.getInt(idx))
          case LongType => provider.resolve(DataType.Int8).handle(buffer, r.getLong(idx))
          case FloatType => provider.resolve(DataType.SinglePrecision).handle(buffer, r.getFloat(idx))
          case DoubleType => provider.resolve(DataType.DoublePrecision).handle(buffer, r.getDouble(idx))
          case DecimalType() => provider.resolve(DataType.DoublePrecision).handle(buffer, r.getDecimal(idx))
          case StringType => provider.resolve(DataType.Text).handle(buffer, StringUtils.removeNullCharacter(r.getString(idx)))
          case BinaryType => provider.resolve(DataType.Bytea).handle(buffer, r.get(idx).asInstanceOf[Array[Byte]])
          case DateType => provider.resolve(DataType.Date).handle(buffer, r.getDate(idx).toLocalDate)
          case TimestampType => provider.resolve(DataType.Timestamp).handle(buffer, r.getTimestamp(idx).toLocalDateTime)

          // Using json for maps and structs
          case MapType(_, _, _) => provider.resolve(DataType.Jsonb).handle(buffer, StringUtils.removeNullCharacter(r.getString(idx)))
          case StructType(_) => provider.resolve(DataType.Jsonb).handle(buffer, StringUtils.removeNullCharacter(r.getString(idx)))

          case ArrayType(BooleanType, _) => makeCollectionValueHandler[java.lang.Boolean, util.Collection[java.lang.Boolean]](DataType.Boolean).handle(buffer, handleArray(r.getSeq[Boolean](idx), Boolean.box))
          // No tinyint in postgres, converting to smallint instead
          case ArrayType(ByteType, _) => makeCollectionValueHandler[java.lang.Short, util.Collection[java.lang.Short]](DataType.Int2).handle(buffer, handleArray(r.getSeq[Byte](idx), (b: Byte) => Short.box(b.toShort)))
          case ArrayType(ShortType, _) => makeCollectionValueHandler[java.lang.Short, util.Collection[java.lang.Short]](DataType.Int2).handle(buffer, handleArray(r.getSeq[Short](idx), Short.box))
          case ArrayType(IntegerType, _) => makeCollectionValueHandler[java.lang.Integer, util.Collection[java.lang.Integer]](DataType.Int4).handle(buffer, handleArray(r.getSeq[Int](idx), Int.box))
          case ArrayType(LongType, _) => makeCollectionValueHandler[java.lang.Long, util.Collection[java.lang.Long]](DataType.Int8).handle(buffer, handleArray(r.getSeq[Long](idx), Long.box))
          case ArrayType(FloatType, _) => makeCollectionValueHandler[java.lang.Float, util.Collection[java.lang.Float]](DataType.SinglePrecision).handle(buffer, handleArray(r.getSeq[Float](idx), Float.box))
          case ArrayType(DoubleType, _) => makeCollectionValueHandler[java.lang.Double, util.Collection[java.lang.Double]](DataType.DoublePrecision).handle(buffer, handleArray(r.getSeq[Double](idx), Double.box))
          case ArrayType(DecimalType(), _) => makeCollectionValueHandler[java.math.BigDecimal, util.Collection[java.math.BigDecimal]](DataType.DoublePrecision).handle(buffer, handleArray(r.getSeq[java.math.BigDecimal](idx), (v: java.math.BigDecimal) => v))
          case ArrayType(StringType, _) => makeCollectionValueHandler[java.lang.String, util.Collection[java.lang.String]](DataType.Text).handle(buffer, handleArray(r.getSeq[String](idx), StringUtils.removeNullCharacter))
          case ArrayType(BinaryType, _) => makeCollectionValueHandler[Array[Byte], util.Collection[Array[Byte]]](DataType.Bytea).handle(buffer, handleArray(r.getSeq[Array[Byte]](idx), (v: Array[Byte]) => v))
          case ArrayType(DateType, _) => makeCollectionValueHandler[java.time.LocalDate, util.Collection[java.time.LocalDate]](DataType.Date).handle(buffer, handleArray(r.getSeq[Date](idx), (v: Date) => v.toLocalDate))
          case ArrayType(TimestampType, _) => makeCollectionValueHandler[java.time.LocalDateTime, util.Collection[java.time.LocalDateTime]](DataType.Timestamp).handle(buffer, handleArray(r.getSeq[Timestamp](idx), (t: Timestamp) => t.toLocalDateTime))

          // TODO -- Add multidimensional arrays ?
          // Not treated: CalendarIntervalType, ObjectType (JVM object), NullType (NULL values type), HiveStringType (internal), UserDefinedType (internal), AnyType
          case _ => throw new UnsupportedOperationException("Trying to insert a datatype not supported by PgBulkInsert")
        }
      }
    }
  }

  def makeRowWriter(schema: StructType): Map[Int, ((Row, DataOutputStream) => Unit)] = {
    schema.fields.toSeq.zipWithIndex.map {case (sf: StructField, idx: Int) =>
      idx -> makeFieldWriter(sf.dataType, idx)
    }.toMap
  }

}