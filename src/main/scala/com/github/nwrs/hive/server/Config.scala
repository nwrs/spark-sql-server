package com.github.nwrs.hive.server

import org.rogach.scallop.ScallopConf
import scala.io.Source

class Config(args:Array[String]) extends ScallopConf(args) {
    banner("""|
              |Custom Spark Hive SQL Server [github.com/nwrs/spark-sql-server]
              |
              |Usage:
              |""".stripMargin)
    val tableConfig = opt[String]("tableConfig", descr = "Table configuration file.", short = 't', argName="/path/to/TableConfig.conf")
    val restPort = opt[Int]("restPort", descr = "Rest port, defaults to 8181.", short = 'p', default = Some(8181), argName="n")
    val jdbcPort = opt[Int]("jdbcPort", descr = "Hive JDBC endpoint port, defaults to 10000.", short = 'j', default = Some(10000), argName="n")
    val sparkOpts = opt[String]("sparkOpts", descr = "Additional Spark options for when running standalone.", short = 'k', argName="opt=value,opt=value,...")
    val master = opt[String]("master", descr = "Spark master, defaults to 'local[*]'.", short = 'm', default= Some("local[*]"), argName="spark://host:port")
    val appName = opt[String]("appName", descr = "Spark application name.", short = 'n', default= Some("Hive SQL Server"), argName="app_name")
    helpWidth(120)
    verify()
}

object Config {
  def getTablesFromConfigFile(configFile:String):Map[String,String] = {
    Source.fromFile(configFile)
      .getLines()
      .filter(!_.startsWith("#"))
      .map(l => {
          val table = l.trim.split("=")
          (table(0).trim, table(1).trim)
      })
      .toMap
  }
}