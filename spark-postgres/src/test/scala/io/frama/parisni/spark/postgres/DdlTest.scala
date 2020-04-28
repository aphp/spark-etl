package io.frama.parisni.spark.postgres

import java.sql.Timestamp
import java.sql.Date
import java.util

import org.apache.spark.sql.QueryTest
import org.junit.Test
import org.postgresql.util.PSQLException

class DdlTest extends QueryTest with SparkSessionTestWrapper {

  @Test def verifySpark(): Unit = {
    spark.sql("select 1").show
  }

  @Test def verifyPostgres() { // Uses JUnit-style assertions
    println(pg.getEmbeddedPostgres.getJdbcUrl("postgres", "pg"))
    val con = pg.getEmbeddedPostgres.getPostgresDatabase.getConnection
    con.createStatement().executeUpdate("create table test(i int)")
    val res = con.createStatement().executeQuery("select 27")
    while (res.next())
      println(res.getInt(1))
  }

  @Test def verifySparkPostgres(): Unit = {

    val input = spark.sql("select 1 as t")
    input
      .write.format("io.frama.parisni.spark.postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val output = spark.read.format("io.frama.parisni.spark.postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("query", "select * from test_table")
      .load

    checkAnswer(input, output)
  }

  @Test def verifySparkPostgresOldDatasource(): Unit = {

    val input = spark.sql("select 1 as t")
    input
      .write.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val output = spark.read.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("query", "select * from test_table")
      .load

    checkAnswer(input, output)
  }

  @Test
  def verifyPostgresConnectionWithUrl(): Unit = {

    val input = spark.sql("select 2 as t")
    input
      .write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

  }

  @Test
  def verifyPostgresConnection() {
    val pg = PGTool(spark, getPgUrl, "/tmp")
      .setPassword("postgres")
    pg.showPassword()
    pg.sqlExecWithResult("select 1").show

  }

  @Test
  def verifyPostgresConnectionFailWhenBadPassword() {
    assertThrows[Exception](
      spark.sql("select 2 as t")
        .write.format("io.frama.parisni.spark.postgres")
        .option("host", "localhost")
        .option("port", pg.getEmbeddedPostgres.getPort)
        .option("database", "postgres")
        .option("user", "idontknow")
        .option("password", "badpassword")
        .option("table", "test_table")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save
    )
  }

  @Test
  def verifyPostgresCreateTable(): Unit = {
    import spark.implicits._
    val schema = ((1, "asdf", 1L, Array(1, 2, 3), Array("bob"), Array(1L, 2L)) :: Nil)
      .toDF("int_col", "string_col", "long_col", "array_int_col", "array_string_col", "array_bigint_col").schema
    getPgTool().tableCreate("test_array", schema, isUnlogged = true)
  }

  @Test
  def verifyPostgresCreateSpecialTable(): Unit = {
    import spark.implicits._
    val data = ((1, "asdf", 1L, Array(1, 2, 3), Array("bob"), Array(1L, 2L)) :: Nil)
      .toDF("INT_COL", "STRING_COL", "LONG_COL", "ARRAY_INT_COL", "ARRAY_STRING_COL", "ARRAY_BIGINT_COL")
    val schema = data.schema
    getPgTool().tableCreate("TEST_ARRAY", schema, isUnlogged = true)
    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", "TEST_ARRAY")
      .save
  }

  @Test
  def verifyPostgresCopyTableConstraint(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute(
      """
        |CREATE TABLE base_table_for_constraints(
        |  constraint_val INT CONSTRAINT on_constraint_value CHECK (constraint_val > 0)
        |)
      """.stripMargin
    )

    val expectedConstraintName = "on_constraint_value"
    val expectedConstraintSrc = "(constraint_val > 0)"

    val constraintSql = """
                          |SELECT
                          |  con.conname,
                          |  con.consrc
                          |FROM pg_catalog.pg_constraint con
                          |  INNER JOIN pg_catalog.pg_class rel
                          |    ON rel.oid = con.conrelid
                          |      AND rel.relname = 'TABLE_NAME';
                        """.stripMargin

    // Assert base table constraint info
    val rsBase = conn.createStatement().executeQuery(constraintSql.replace("TABLE_NAME", "base_table_for_constraints"))
    rsBase.next()
    assert(rsBase.getString(1) == expectedConstraintName)
    assert(rsBase.getString(2) == expectedConstraintSrc)

    // Do the copy
    getPgTool().tableCopy("base_table_for_constraints", "copy_table_for_constraints", copyConstraints = true)

    // Assert copied table has the storage parameter
    val rsCopy = conn.createStatement().executeQuery(constraintSql.replace("TABLE_NAME", "copy_table_for_constraints"))
    rsCopy.next()
    assert(rsCopy.getString(1) == expectedConstraintName)
    assert(rsCopy.getString(2) == expectedConstraintSrc)
  }

  @Test
  def verifyPostgresCopyTableIndexes(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute(
      """
        |CREATE TABLE base_table_for_indexes(
        |  compounded_idx_1 INT,
        |  compounded_idx_2 VARCHAR(256)
        |)
      """.stripMargin
    )

    // Add complex index
    conn.createStatement().execute(
      """
        |CREATE INDEX compounded_idx ON base_table_for_indexes USING btree(
        |  compounded_idx_1 ASC NULLS FIRST,
        |  compounded_idx_2 DESC
        |) WHERE LENGTH(compounded_idx_2) < 10
      """.stripMargin
    )

    val expectedIndexDef = "CREATE INDEX IDX_NAME ON public.TABLE_NAME USING btree " +
                           "(compounded_idx_1 NULLS FIRST, compounded_idx_2 DESC) " +
                           "WHERE (length((compounded_idx_2)::text) < 10)"
    // Assert base index info is correct
    val rsBase = conn.createStatement().executeQuery("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'base_table_for_indexes'")
    rsBase.next()
    assert(rsBase.getString(1) == "compounded_idx")
    assert(rsBase.getString(2) == expectedIndexDef.replace("IDX_NAME", "compounded_idx").replace("TABLE_NAME", "base_table_for_indexes"))

    // Do the copy
    getPgTool().tableCopy("base_table_for_indexes", "copy_table_for_indexes", copyIndexes = true)

    // Assert copied index info is correct
    val rsCopy = conn.createStatement().executeQuery("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'copy_table_for_indexes'")
    rsCopy.next()
    val idxName = rsCopy.getString(1)
    assert(rsCopy.getString(2) == expectedIndexDef.replace("IDX_NAME", idxName).replace("TABLE_NAME", "copy_table_for_indexes"))

  }

  @Test
  def verifyPostgresCopyTableStorage(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute(
      """
        |CREATE TABLE base_table_for_storage(
        | toast_column VARCHAR(1024)
        |)
      """.stripMargin)

    val checkStorageSql =
      """
        |select t2.attstorage
        |from pg_class t1
        |inner join pg_attribute t2
        |  on t1.oid = t2.attrelid
        |  and t1.relname = 'TABLE_NAME'
        |  and t2.attname = 'COLUMN_NAME'
      """.stripMargin

    // Assert that original column has storage x
    val rsBaseOriginal = conn.createStatement().executeQuery(checkStorageSql.replace("TABLE_NAME", "base_table_for_storage").replace("COLUMN_NAME", "toast_column"))
    rsBaseOriginal.next()
    assert(rsBaseOriginal.getString(1) == "x")

    conn.createStatement().execute(
      """
        |ALTER TABLE base_table_for_storage ALTER COLUMN toast_column SET STORAGE PLAIN
      """.stripMargin)

    // Assert that updated has storage p
    val rsBaseUpdated = conn.createStatement().executeQuery(checkStorageSql.replace("TABLE_NAME", "base_table_for_storage").replace("COLUMN_NAME", "toast_column"))
    rsBaseUpdated.next()
    assert(rsBaseUpdated.getString(1) == "p")

    // Do the copy
    getPgTool().tableCopy("base_table_for_storage", "copy_table_for_storage", copyStorage = true)

    // Assert that copied-table column has storage p
    val rsCopy = conn.createStatement().executeQuery(checkStorageSql.replace("TABLE_NAME", "copy_table_for_storage").replace("COLUMN_NAME", "toast_column"))
    rsCopy.next()
    assert(rsCopy.getString(1) == "p")
  }

  @Test
  def verifyPostgresCopyTableComments(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute(
      """
        |CREATE TABLE base_table_for_comments(comment_val int)
      """.stripMargin
    )

    // Add comment
    conn.createStatement().execute(
      """
        |COMMENT ON COLUMN base_table_for_comments.comment_val IS 'Test comment'
      """.stripMargin
    )

    val commentsSql =
      """
        |SELECT c.column_name,pgd.description
        |FROM pg_catalog.pg_description pgd
        |    INNER JOIN information_schema.columns c
        |      ON (pgd.objsubid=c.ordinal_position)
        |WHERE c.table_name='TABLE_NAME'
      """.stripMargin

    val expectedColumnName = "comment_val"
    val expectedColumnComment = "Test comment"

    // Assert base index info is correct
    val rsBase = conn.createStatement().executeQuery(commentsSql.replace("TABLE_NAME", "base_table_for_comments"))
    rsBase.next()
    assert(rsBase.getString(1) == expectedColumnName)
    assert(rsBase.getString(2) == expectedColumnComment)

    // Do the copy
    getPgTool().tableCopy("base_table_for_comments", "copy_table_for_comments", copyComments = true)

    // Assert copied index info is correct
    val rsCopy = conn.createStatement().executeQuery(commentsSql.replace("TABLE_NAME", "copy_table_for_comments"))
    rsCopy.next()
    assert(rsCopy.getString(1) == expectedColumnName)
    assert(rsCopy.getString(2) == expectedColumnComment)
  }

  @Test
  def verifyPostgresCopyTableOwner(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()

    conn.createStatement().execute("CREATE TABLE base_table_for_owner()")
    conn.createStatement().execute("CREATE ROLE ru1 LOGIN")
    conn.createStatement().execute("ALTER TABLE base_table_for_owner OWNER TO ru1")

    getPgTool().tableCopy("base_table_for_owner", "copy_table_for_owner", copyOwner = true)

    // Check table perms
    val rsBaseTableOwner = conn.createStatement().executeQuery("SELECT relowner FROM pg_class WHERE relname = 'base_table_for_owner'")
    rsBaseTableOwner.next()
    val baseTableOwner = rsBaseTableOwner.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsBaseTableOwner.close()

    val rsCopyTableOwner = conn.createStatement().executeQuery("SELECT relowner FROM pg_class WHERE relname = 'copy_table_for_owner'")
    rsCopyTableOwner.next()
    val copyTableOwner = rsCopyTableOwner.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsCopyTableOwner.close()

    assert(baseTableOwner == copyTableOwner)

  }

  @Test
  def verifyPostgresCopyTablePermissions(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()

    conn.createStatement().execute("CREATE TABLE base_table_for_perms(perm_col int)")

    conn.createStatement().execute("CREATE ROLE ru1 LOGIN INHERIT")
    conn.createStatement().execute("CREATE ROLE rg1 NOINHERIT")
    conn.createStatement().execute("CREATE ROLE rg2 NOINHERIT")

    conn.createStatement().execute("GRANT rg1 TO ru1")
    conn.createStatement().execute("GRANT rg2 TO rg1")

    conn.createStatement().execute("GRANT SELECT ON base_table_for_perms TO PUBLIC")
    conn.createStatement().execute("GRANT INSERT ON base_table_for_perms TO ru1")
    conn.createStatement().execute("GRANT UPDATE ON base_table_for_perms TO ru1 WITH GRANT OPTION")
    conn.createStatement().execute("GRANT INSERT ON base_table_for_perms TO rg1")
    conn.createStatement().execute("GRANT TRUNCATE ON base_table_for_perms TO rg2")

    conn.createStatement().execute("GRANT INSERT(perm_col) ON base_table_for_perms TO ru1")
    conn.createStatement().execute("GRANT UPDATE(perm_col) ON base_table_for_perms TO rg1")

    getPgTool().tableCopy("base_table_for_perms", "copy_table_for_perms", copyPermissions = true)

    // Check table perms
    val rsBaseTablePerms = conn.createStatement().executeQuery("SELECT relacl FROM pg_class WHERE relname = 'base_table_for_perms'")
    rsBaseTablePerms.next()
    val baseTablePerms = rsBaseTablePerms.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsBaseTablePerms.close()

    val rsCopyTablePerms = conn.createStatement().executeQuery("SELECT relacl FROM pg_class WHERE relname = 'copy_table_for_perms'")
    rsCopyTablePerms.next()
    val copyTablePerms = rsCopyTablePerms.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsCopyTablePerms.close()

    baseTablePerms.foreach(p => assert(copyTablePerms.contains(p)))


    // Check columns perms
    val rsBaseTableColumnsPerms = conn.createStatement().executeQuery(
      """
        SELECT attacl
        |FROM pg_attribute a
        |  JOIN pg_class c
        |    ON a.attrelid = c.oid
        |WHERE c.relname = 'base_table_for_perms'
        |  AND a.attname = 'perm_col'
      """.stripMargin)
    rsBaseTableColumnsPerms.next()
    val baseTableColumnPerms = rsBaseTableColumnsPerms.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsBaseTableColumnsPerms.close()

    val rsCopyTableColumnPerms = conn.createStatement().executeQuery(
      """
        SELECT attacl
        |FROM pg_attribute a
        |  JOIN pg_class c
        |    ON a.attrelid = c.oid
        |WHERE c.relname = 'copy_table_for_perms'
        |  AND a.attname = 'perm_col'
      """.stripMargin
    )
    rsCopyTableColumnPerms.next()
    val copyTableColumnPerms = rsCopyTableColumnPerms.getString(1).drop(1).dropRight(1).split(",").map(p => p.split("/")(0)).toSeq
    rsCopyTableColumnPerms.close()

    baseTableColumnPerms.foreach(p => assert(copyTableColumnPerms.contains(p)))

  }


  @Test
  def verifyKillLocks(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute("create table lockable()")
    conn.setAutoCommit(false)
    try {
      conn.createStatement().execute("BEGIN TRANSACTION")
      conn.createStatement().execute("LOCK lockable IN ACCESS EXCLUSIVE MODE")
      assert(getPgTool().killLocks("lockable") == 1)
      conn.commit()
      fail()
    } catch {
      case e: PSQLException => ()
    }
  }

  @Test
  def verifyRename(): Unit = {
    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute("create table to_rename()")
    getPgTool().tableRename("to_rename", "renamed")

    var rs = conn.createStatement().executeQuery("SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = 'to_rename')")
    rs.next()
    assert(! rs.getBoolean(1))

    rs = conn.createStatement().executeQuery("SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = 'renamed')")
    rs.next()
    assert(rs.getBoolean(1))
    conn.close()
  }

  @Test
  def verifyPostgresStreamBulkLoadMode(): Unit = {
    import spark.implicits._
    val data = ((1, "asdf", 1L) :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL")
    val schema = data.schema
    getPgTool().tableCreate("TEST_STREAM_BULK_LOAD", schema, isUnlogged = true)
    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", "TEST_STREAM_BULK_LOAD")
      .option("bulkLoadMode", "stream")
      .save
  }

  @Test
  def verifyPostgresStreamBulkLoadModeError(): Unit = {
    import spark.implicits._
    val fakeData = ((1, "asdf", 1L) :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL")
    val schema = fakeData.schema
    val data = ((1, "asdf", 1L, "err") :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL", "ERR_COL")

    getPgTool().tableCreate("TEST_STREAM_BULK_LOAD", schema, isUnlogged = true)

    assertThrows[RuntimeException](
      data.write.format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("type", "full")
        .option("table", "TEST_STREAM_BULK_LOAD")
        .option("bulkLoadMode", "stream")
        .save
    )

  }


  def verifyPostgresPgBinaryLoadMode(loadMode: String): Unit = {
    import spark.implicits._
    val data = ((1, "asdf", 1L) :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL")
    val schema = data.schema
    val tableName = s"TEST_PG_BINARY_LOAD_$loadMode"
    getPgTool().tableCreate(tableName, schema, isUnlogged = true)
    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", tableName)
      .option("bulkLoadMode", loadMode)
      .save
  }
  @Test
  def verifyPostgresPgBinaryLoadStream() = verifyPostgresPgBinaryLoadMode("PgBinaryStream")
  @Test
  def verifyPostgresPgBinaryLoadFiles() = verifyPostgresPgBinaryLoadMode("PgBinaryFiles")


  def verifyPostgresPgBinaryCreateSpecialTable(loadMode: String): Unit = {
    import spark.implicits._
    val data = ((1, "asdf", 1L, Array(1, 2, 3), Array("bob"), Array(1L, 2L)) :: Nil)
      .toDF("INT_COL", "STRING_COL", "LONG_COL", "ARRAY_INT_COL", "ARRAY_STRING_COL", "ARRAY_BIGINT_COL")
    val schema = data.schema
    val tableName = s"TEST_ARRAY_$loadMode"
    getPgTool().tableCreate(tableName, schema, isUnlogged = true)
    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", tableName)
      .option("bulkLoadMode", loadMode)
      .save
  }
  @Test
  def verifyPostgresPgBinaryStreamCreateSpecialTable() = verifyPostgresPgBinaryCreateSpecialTable("PgBinaryStream")
  @Test
  def verifyPostgresPgBinaryFilesCreateSpecialTable() = verifyPostgresPgBinaryCreateSpecialTable("PgBinaryFiles")


  def verifyPgBinaryLoadModeError(loadMode: String): Unit = {
    import spark.implicits._
    val fakeData = ((1, "asdf", 1L) :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL")
    val schema = fakeData.schema
    val data = ((1, "asdf", 1L, "err") :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL", "ERR_COL")
    val tableName = s"TEST_PG_BINARY_ERR_$loadMode"
    getPgTool().tableCreate(tableName, schema, isUnlogged = true)

    assertThrows[RuntimeException](
      data.write.format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("type", "full")
        .option("table", tableName)
        .option("bulkLoadMode", loadMode)
        .save
    )
  }
  @Test
  def verifyPgBinaryLoadStreamModeError() = verifyPgBinaryLoadModeError("PgBinaryStream")
  @Test
  def verifyPgBinaryLoadFilesModeError() = verifyPgBinaryLoadModeError("PgBinaryFiles")

  val testDate = new Date(9)
  val testTimestamp = new Timestamp(10L)

  case class InnerStruct(fi1: Int, fi2: String)

  def verifyPgBinaryLoadPrimitiveTypes(loadMode: String): Unit = {
    import spark.implicits._
    // Needed for the InnerStruct class to be converted to DataFrame through implicit
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
    val data = Seq(
      (
        true,
        2.toByte,
        3.toShort,
        4,
        5L,
        6.0f,
        7.0d,
        "8",
        testDate,
        testTimestamp,
        "11".getBytes(),
        BigDecimal(12.0),
        Map("test_key" -> 13L),
        InnerStruct(14, "test_fi2")
      )
    ).toDF("BOOL_COL", "BYTE_COL", "SHORT_COL", "INT_COL", "LONG_COL", "FLOAT_COL",
           "DOUBLE_COL", "STRING_COL", "DATE_COL", "TIMESTAMP_COL", "BYTEA_COL",
           "BIGD_COL", "MAP_COL", "STRUCT_COL")

    val schema = data.schema
    val tableName = s"TEST_PG_BINARY_LOAD_PRIMITIVES_$loadMode"

    getPgTool().tableCreate(tableName, schema, isUnlogged = true)

    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", tableName)
      .option("bulkLoadMode", loadMode)
      .save

    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    val rs = conn.createStatement().executeQuery(s"""SELECT * FROM "$tableName" """)
    rs.next()
    assert(rs.getBoolean(1))
    assert(rs.getByte(2) == 2)
    assert(rs.getShort(3) == 3)
    assert(rs.getInt(4) == 4)
    assert(rs.getLong(5) == 5L)
    assert(rs.getFloat(6) == 6.0f)
    assert(rs.getDouble(7) == 7.0d)
    assert(rs.getString(8) == "8")
    assert(rs.getDate(9).toLocalDate.equals(testDate.toLocalDate))
    assert(rs.getTimestamp(10).equals(testTimestamp))
    assert(util.Arrays.equals(rs.getBytes(11), "11".getBytes()))
    assert(rs.getDouble(12) == 12.0d)
    assert(rs.getString(13) == "{\"test_key\": 13}")
    assert(rs.getString(14) == "{\"fi1\": 14, \"fi2\": \"test_fi2\"}")
  }
  @Test
  def verifyPgBinaryStreamLoadPrimitiveTypes() = verifyPgBinaryLoadPrimitiveTypes("PgBinaryStream")
  @Test
  def verifyPgBinaryFilesLoadPrimitiveTypes() = verifyPgBinaryLoadPrimitiveTypes("PgBinaryFiles")


  def verifyPgBinaryLoadNullPrimitives(loadMode: String): Unit = {
    import spark.implicits._
    // Needed for the InnerStruct class to be converted to DataFrame through implicit
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
    val data = Seq(
      (
        None.asInstanceOf[Option[Boolean]],
        None.asInstanceOf[Option[Byte]],
        None.asInstanceOf[Option[Short]],
        None.asInstanceOf[Option[Int]],
        None.asInstanceOf[Option[Long]],
        None.asInstanceOf[Option[Float]],
        None.asInstanceOf[Option[Double]],
        None.asInstanceOf[Option[String]],
        None.asInstanceOf[Option[Date]],
        None.asInstanceOf[Option[Timestamp]],
        None.asInstanceOf[Option[Array[Byte]]],
        None.asInstanceOf[Option[BigDecimal]],
        None.asInstanceOf[Option[Map[String, Long]]],
        None.asInstanceOf[Option[InnerStruct]]
      )
    ).toDF("BOOL_COL", "BYTE_COL", "SHORT_COL", "INT_COL", "LONG_COL", "FLOAT_COL",
           "DOUBLE_COL", "STRING_COL", "DATE_COL", "TIMESTAMP_COL", "BYTEA_COL",
           "BIGD_COL", "MAP_COL", "STRUCT_COL")

    val schema = data.schema
    val tableName = s"TEST_PG_BINARY_LOAD_NULL_PRIMITIVES_$loadMode"

    getPgTool().tableCreate(tableName, schema, isUnlogged = true)

    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", tableName)
      .option("bulkLoadMode", loadMode)
      .save

    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    val rs = conn.createStatement().executeQuery(s"""SELECT * FROM "$tableName" """)
    rs.next()
    (1 to 14).foreach(i => assert(rs.getString(i) == null))

  }
  @Test
  def verifyPgBinaryStreamLoadNullPrimitives() = verifyPgBinaryLoadNullPrimitives("PgBinaryStream")
  @Test
  def verifyPgBinaryFilesLoadNullPrimitives() = verifyPgBinaryLoadNullPrimitives("PgBinaryFiles")


  def verifyPgBinaryLoadNullInArrays(loadMode: String): Unit = {
    import spark.implicits._
    // Needed for the InnerStruct class to be converted to DataFrame through implicit
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
    val colNames = Seq("BOOL_COL", "BYTE_COL", "SHORT_COL", "INT_COL", "LONG_COL", "FLOAT_COL",
                       "DOUBLE_COL", "STRING_COL","DATE_COL", "TIMESTAMP_COL", "BYTEA_COL",
                       "BIGD_COL")

    val data = Seq(
      (
        Array(Some(true), None),
        Array(Some(2.toByte), None),
        Array(Some(3.toShort), None),
        Array(Some(4), None),
        Array(Some(5L), None),
        Array(Some(6.0f), None),
        Array(Some(7.0d), None),
        Array(Some("8"), None),
        Array(Some(testDate), None),
        Array(Some(testTimestamp), None),
        Array(Some("11".getBytes()), None),
        Array(Some(BigDecimal(12)), None)
      )
    ).toDF(colNames: _* )

    val schema = data.schema
    val tableName = s"TEST_PG_BULK_INSERT_LOAD_NULL_IN_ARRAYS_$loadMode"

    getPgTool().tableCreate(tableName, schema, isUnlogged = true)

    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", tableName)
      .option("bulkLoadMode", loadMode)
      .save

    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()

    val selectFirstValues = colNames.map(n => PGTool.sanP(n) + "[1]").mkString(", ")
    val selectSecondValues = colNames.map(n => PGTool.sanP(n) + "[2]").mkString(", ")
    val rs = conn.createStatement().executeQuery(
      s"""
        |SELECT
        |  $selectFirstValues,
        |  $selectSecondValues
        |FROM "$tableName"
      """.stripMargin)
    rs.next()

    assert(rs.getBoolean(1))
    assert(rs.getShort(2) == 2)
    assert(rs.getShort(3) == 3)
    assert(rs.getInt(4) == 4)
    assert(rs.getLong(5) == 5L)
    assert(rs.getFloat(6) == 6.0f)
    assert(rs.getDouble(7) == 7.0d)
    assert(rs.getString(8) == "8")
    assert(rs.getDate(9).toLocalDate.equals(testDate.toLocalDate))
    assert(rs.getTimestamp(10).toLocalDateTime.equals(testTimestamp.toLocalDateTime))
    assert(util.Arrays.equals(rs.getBytes(11), "11".getBytes))
    assert(rs.getDouble(12) == 12.0d)

    (13 to 24).foreach(i => assert(rs.getString(i) == null))

  }
  @Test
  def verifyPgBinaryStreamLoadNullInArrays() = verifyPgBinaryLoadNullInArrays("PgBinaryStream")
  @Test
  def verifyPgBinaryFilesLoadNullInArrays() = verifyPgBinaryLoadNullInArrays("PgBinaryFiles")

}
