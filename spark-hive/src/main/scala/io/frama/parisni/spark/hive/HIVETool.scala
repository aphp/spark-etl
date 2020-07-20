/*
package io.frama.parisni.spark.hive

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet

/**
 *  Factory for [[io.frama.parisni.omop.deid.Extract]] instances.
 *  @constructor Pass a "principal", a "keytab" and a "url"
 */

class HIVETool(principal: String, keytab: String, url: String) {
  private var connection: Connection = null

  /**
 * Creates the connection
 *
 */
  protected def init() = {
    val conf = new org.apache.hadoop.conf.Configuration();
    conf.set("hadoop.security.authentication", "Kerberos");
    org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
    org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal, keytab);
    val driver = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)
    connection = DriverManager.getConnection(url)
  }

  /**
 * Run a query without expecting a result set back
 *
 * @param query String A sql statement
 */
  def execute(query: String) = {
    val statement = connection.createStatement()
    statement.execute(query)
  }

  /**
 * Run a query without and returns a resultset
 *
 * @param query String A sql statement
 */
  def executeQuery(query: String): ResultSet = {
    val statement = connection.createStatement()
    statement.executeQuery(query)
  }

  /**
 * Close the pending connection
 *
 */
  def close() = {
    connection.close()
  }

  /**
 * This produce a merge statement. It can be an insert, an
 * update and/or a delete merge depending on the specified mode.
 * In each case, different the arguments need to be specified.
 * The merge is always based on a key comparison that can be a
 * list of keys.
 *
 * @param targetTable String The table to be merged
 * @param sourceTable String The table with new data
 * @param key List[String] The list of key to compare source and target
 * @param colums List[String] The colums list in the exact order
 * @param mode String i, u, d for Insert, Update, or Delete. "iud" if all
 * @param compareColumns Option[List[String]] The list of the colums to trigger the update
 * @param updateColumns  Option[List[String]] The list of the columns to be updated
 * @param deleteClause  Option[String] The statement triggering the delete
 *
 * @note the TARGET table SHALL have the exact same columns in the **same order** than the TARGET TABLE
 */
  def merge(targetTable: String, sourceTable: String, key: List[String], columns: Option[List[String]], mode: String, compareColumns: Option[List[String]] = None, updateColumns: Option[List[String]] = None, deleteClause: Option[String] = None) = {
    val mergeQuery = HIVETool.merge(targetTable, sourceTable, key, columns, mode, compareColumns, updateColumns, deleteClause)
    execute(mergeQuery)
  }

}

object HIVETool {

  def apply(principal: String, keytab: String, url: String) = {
    val hiveJdbc = new HIVETool(principal, keytab, url)
    hiveJdbc.init()
    hiveJdbc
  }

  /**
 * @example given a, b columns from source table and a, c from source table
 * produces a = t.a, b = t.b
 */
  def generateUpdateSet(sourceTable: String, columns: List[String]): String = {
    columns.map(c => f""""$c" = "$sourceTable"."$c"""").mkString(", ")
  }

  /**
 * @example given a, b columns from source table and a, c from source table
 * produces a <> t.a OR b <> t.b
 */
  def generateUpdateCompare(sourceTable: String, columns: List[String]): String = {
    columns.map(c => f""""$c" <> "$sourceTable"."$c"""").mkString(" OR ")
  }

  /**
 * @example given a, b columns from source table and a, c from source table
 * produces s.a = t.a AND s.b = t.b
 */
  def generateJoin(sourceTable: String, targetTable: String, key: List[String]): String = {
    key.map(c => f""""$sourceTable"."$c" = "$targetTable"."$c"""").mkString(" AND ")
  }

  def generateInsertSelect(table: String, colums: List[String]): String = {
    colums.map(c => f""" "$table"."$c" """).mkString(", ")
  }

  def merge(targetTable: String, sourceTable: String, key: List[String], columns: Option[List[String]], mode: String, compareColumns: Option[List[String]] = None, updateColumns: Option[List[String]] = None, deleteClause: Option[String] = None): String = {
    require(!mode.contains("i|u|d".r), "mode SHALL contain i, u and/ d")
    require(!mode.contains("i") || (columns.isDefined), "when mode i then define the columns")
    require(!mode.contains("d") || (deleteClause.isDefined), "when mode d then define the deleteClause")
    require(!mode.contains("u") || (updateColumns.isDefined && compareColumns.isDefined), "when mode u then define both the update and delete")
    val updateWhere = compareColumns match {
      case None => ""
      case _    => generateUpdateCompare(sourceTable, compareColumns.get)

    }

    val updateSet = updateColumns match {
      case None => ""
      case _    => generateUpdateSet(sourceTable, updateColumns.get)

    }
    val insert = columns match {
      case None => ""
      case _    => generateInsertSelect(sourceTable, columns.get)

    }

    val insertStr = if (mode.contains("i"))
      f"""
      WHEN NOT MATCHED
        THEN INSERT VALUES ($insert)"""

    val updateStr = if (mode.contains("u"))
      f"""
      WHEN MATCHED AND ($updateWhere)
        THEN UPDATE SET $updateSet"""
    else
      ""

    val deleteStr = if (mode.contains("d"))
      f"""
      WHEN NOT MATCHED AND ${deleteClause.get}
        THEN DELETE"""
    else
      ""

    val joinStr = generateJoin(sourceTable, targetTable, key)
    f"""
      MERGE INTO "$targetTable"
      USING "$sourceTable"
      ON $joinStr$updateStr$deleteStr$insertStr
      """
  }

}

 */
