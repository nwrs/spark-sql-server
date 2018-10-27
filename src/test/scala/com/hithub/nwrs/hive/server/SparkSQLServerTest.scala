package com.hithub.nwrs.hive.server

import java.io.File
import java.sql.DriverManager
import com.github.nwrs.hive.server._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.Future

class SparkSQLServerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val jdbcPort = "10001"
  val restPort = "8182"

  val conf = new Config(Array("--sparkOpts", "spark.executor.memory=500m",
                              "--jdbcPort", jdbcPort,
                              "--restPort", restPort))

  val testTableFilePath:String = new File(".").getCanonicalPath+"/src/test/data/test-table.parquet"
  val testTable2FilePath:String = new File(".").getCanonicalPath+"/src/test/data/test-table-2.parquet"

  val sc = SparkHelper.initContextAndHive(conf)

  override def beforeAll(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Future { App.startRestService(conf) }
  }

  override def afterAll(): Unit = sc.stop()

  behavior of "A Spark SQL Server"

  it should "register and de-register Parquet files as tables" in {

    // register a table
    SparkHelper.registerTable("test_table", testTableFilePath)

    // check registered
    sc.catalog.tableExists("test_table") should be (true)
    val testTable = sc.table("test_table")
    testTable.count() should equal (10)

    // de-register
    SparkHelper.deregisterTable("test_table")
    sc.catalog.tableExists("test_table") should be (false)

    // register multiple tables
    SparkHelper.registerTables(Map[String,String]("test_table" -> testTableFilePath, "test_table_two" -> testTable2FilePath))
    sc.catalog.listTables().count() should equal (2)

    // check registered
    sc.catalog.tableExists("test_table") should be (true)
    sc.catalog.tableExists("test_table_two") should be (true)

    // de-register
    SparkHelper.deregisterTable("test_table")
    SparkHelper.deregisterTable("test_table_two")

    // check de-registered
    sc.catalog.listTables().count() should equal (0)
  }


  it should "provide a REST endpoint for registering and de-registering tables" in {
    import RestTestClient._
    val RESTful = new RestTestClient(s"localhost:$restPort")

    // Check no tables registered
    RESTful getAs[Tables]("/api/v1/tables") should equal (success(Tables(Map())))

    // Register table
    RESTful post("/api/v1/table", RegisterTableRqt("test_table", testTableFilePath)) should equal (accepted)

    //check registration with /api/v1/tables
    RESTful getAs[Tables]("/api/v1/tables") should equal (success(Tables(Map("test_table" -> testTableFilePath))))
    sc.catalog.tableExists("test_table") should be (true)

    //check registration with /api/v1/table/test_table
    RESTful getAs[Tables]("/api/v1/table/test_table") should equal (success(Tables(Map("test_table" -> testTableFilePath))))

    // check non existent table for 404
    RESTful get("/api/v1/table/test_table_missing") should equal (notFound("Table 'test_table_missing' not found"))

    // de-register
    RESTful delete("/api/v1/table/test_table") should equal (accepted)

    // check none registered
    RESTful getAs[Tables]("/api/v1/tables") should equal (success(Tables(Map())))
    sc.catalog.tableExists("test_table") should be (false)
  }


  it should "create a valid JDBC endpoint on requested port" in {
   SparkHelper.registerTable("test_table", testTableFilePath)
    val con = DriverManager.getConnection(s"jdbc:hive2://localhost:$jdbcPort/default")
    val statement = con.createStatement()

    statement.execute("select count(*) as cnt from test_table")
    val results = statement.getResultSet
    results.next()
    results.getInt("cnt") should equal (10)

    SparkHelper.deregisterTable("test_table")
    con.close
  }

}
