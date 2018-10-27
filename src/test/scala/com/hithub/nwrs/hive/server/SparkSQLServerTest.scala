package com.hithub.nwrs.hive.server

import java.io.File
import java.sql.DriverManager
import com.github.nwrs.hive.server._
import com.twitter.finagle.Http
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{RequestBuilder, Response}
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import net.liftweb.json.{NoTypeHints, Serialization, parse}
import net.liftweb.json.Serialization.write
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

  override def afterAll(): Unit = {
    sc.stop()
  }

  behavior of "A Spark SQL Server"

  it should "register and de-register Parquet files as tables" in {
    SparkHelper.registerTable("test_table", testTableFilePath)
    sc.catalog.tableExists("test_table") should be (true)

    val testTable = sc.table("test_table")
    testTable.count() should equal (10)

    SparkHelper.deregisterTable("test_table")
    sc.catalog.tableExists("test_table") should be (false)

    SparkHelper.registerTables(Map[String,String]("test_table" -> testTableFilePath, "test_table_two" -> testTable2FilePath))
    sc.catalog.listTables().count() should equal (2)

    sc.catalog.tableExists("test_table") should be (true)
    sc.catalog.tableExists("test_table_two") should be (true)

    SparkHelper.deregisterTable("test_table")
    SparkHelper.deregisterTable("test_table_two")

    sc.catalog.listTables().count() should equal (0)
  }


  it should "provide a REST endpoint for registering and de-registering tables" in {
    implicit val formats = Serialization.formats(NoTypeHints)
    val client =  ClientBuilder()
      .hosts(s"localhost:$restPort")
      .hostConnectionLimit(10)
      .tcpConnectTimeout(Duration.fromSeconds(10))
      .stack(Http.client)
      .build()

    def syncGet(path:String):Response = {
      Await.result(client(RequestBuilder.create().url(s"http://localhost:$restPort/$path").buildGet()))
    }

    def syncPostJson[T](path:String, bodyJsonCaseClass:T):Response = {
      Await.result(client(RequestBuilder.create().url(s"http://localhost:$restPort/$path").buildPost(Buf.Utf8(write[T](bodyJsonCaseClass)))))
    }

    def syncDelete[T](path:String):Response = {
      Await.result(client(RequestBuilder.create().url(s"http://localhost:$restPort/$path").buildDelete()))
    }

    // Check no tables registered
    val getResponse = syncGet("api/v1/tables")
    getResponse.statusCode should equal (200)
    val tables = parse(getResponse.getContentString()).extract[Tables]
    tables.tables.size should equal (0)

    // Register table
    val postResponse = syncPostJson("api/v1/table", RegisterTableRqt("test_table", testTableFilePath))
    postResponse.statusCode should equal (202)

    //check registration with /api/v1/tables
    val registrationRes = syncGet("api/v1/tables")
    registrationRes.statusCode should equal (200)
    val tables1 = parse(registrationRes.getContentString()).extract[Tables]
    tables1 should equal (Tables(Map("test_table" -> testTableFilePath)))
    sc.catalog.tableExists("test_table") should be (true)

    //check registration with /api/v1/table/test_table
    val registrationSingleTableRes = syncGet("api/v1/table/test_table")
    registrationSingleTableRes.statusCode should equal (200)
    val singleTable = parse(registrationSingleTableRes.getContentString()).extract[Tables]
    singleTable should equal (Tables(Map("test_table" -> testTableFilePath)))

    // de-register
    val deregisterResponse = syncDelete("api/v1/table/test_table")
    deregisterResponse.statusCode should equal (202)

    // check none registered
    val nothingRegistered = syncGet("api/v1/tables")
    nothingRegistered.statusCode should equal (200)
    val noTables = parse(nothingRegistered.getContentString()).extract[Tables]
    noTables.tables.size should equal (0)
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
