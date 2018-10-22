package com.github.nwrs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

object App {
  
  // create spark session
  val ss =  SparkSession.builder
    .appName("Spark SQL Server")
    .config("spark.debug.maxToStringFields","100")
    .config("spark.executor.memory","4g")
    .config("spark.sql.hive.thriftServer.singleSession","true") // required so registered views are visible from JDBC connections
    .config("spark.sql.catalogImplementation","hive")
    .master(s"local[*]").getOrCreate()   // simple local master, set to Spark cluster url as required

  // Start hive jdbc endpoint on default port.
  // The JDBC connection string will be "jdbc:hive2://localhost:10000/default"
  // For more on connecting to Hive via JDBC see: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC
  HiveThriftServer2.startWithContext(ss.sqlContext)

  def main(args : Array[String]) {

    // Register a parquet file as a view to allow JDBC connections to query
    ss.read
      .option("basePath", "hdfs://localhost:9000/data/tweets.parquet")
      .option("mergeSchema", "false")
      .parquet("hdfs://localhost:9000/data/tweets.parquet")
      .createTempView("tweets")

    Thread.sleep(Long.MaxValue)

  }

}
