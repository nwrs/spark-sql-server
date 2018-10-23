package com.github.nwrs.hive.server

import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory

object Spark {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  @volatile
  var ss:Option[SparkSession] = None

  def initContextAndHive(conf:Config):SparkSession = {
    if (ss.isEmpty) {
      this.synchronized {
        if (ss.isEmpty) {
          // spark session builder
          val builder = SparkSession.builder
            .appName(conf.appName())
            .config("spark.debug.maxToStringFields", "100")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.hive.thriftServer.singleSession", "true") // required so registered views are visible from JDBC connections
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.server2.thrift.port", conf.jdbcPort())

          // additional user provided options
          if (conf.sparkOpts.isDefined) {
            conf.sparkOpts().split(",").map(_.trim).foreach { o =>
              val optPair = o.split("=").map(_.trim)
              builder.config(optPair(0), optPair(1))
            }
          }

          // create spark session
          log.info(s"Creating Spark session for application '${conf.appName()}' with Spark master '${conf.master}'")
          ss = Some(builder.master(conf.master()).getOrCreate())

          // create Hive Thrift endpoint
          log.info(s"Creating embedded Hive Thrift Server on port ${conf.jdbcPort()}.")
          HiveThriftServer2.startWithContext(sqlContext)
        }
      }
    }
    ss.get
  }

  private[this] def context:SparkSession = {
    if (ss.isDefined)
      ss.get
    else
      throw new Exception("Spark context not initialised")
  }

  private[this] def sqlContext:SQLContext = context.sqlContext

  def registerTable(table:String, file:String, mergeSchema:Boolean = false):Unit = {
    log.info(s"Attempting to register table '$table' using '$file'.")
    context.read
      .option("basePath", file)
      .option("mergeSchema", mergeSchema.toString)
      .parquet(file)
      .createTempView(table)
    log.info(s"Registered table '$table' successfully.")
  }

  def registerTables(tables:Seq[(String,String)]):Unit = tables.foreach(t => registerTable(t._1, t._2))

  def deregisterTable(table:String):Unit = {
    log.info(s"De-registering table '$table'.")
    sqlContext.dropTempTable(table)
  }

}
