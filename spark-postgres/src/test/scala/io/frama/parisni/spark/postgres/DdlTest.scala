package io.frama.parisni.spark.postgres

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
  def verifyPostgresBulkLoadMode(): Unit = {
    import spark.implicits._
    val data = ((1, "asdf", 1L) :: Nil).toDF("INT_COL", "STRING_COL", "LONG_COL")
    val schema = data.schema
    getPgTool().tableCreate("TEST_STREAM_BULK_LOAD", schema, isUnlogged = true)
    data.write.format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "full")
      .option("table", "TEST_ARRAY")
      .option("bulkLoadMode", "stream")
      .save
  }

}
