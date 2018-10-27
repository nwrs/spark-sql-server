package com.github.nwrs.hive.server

import akka.actor.{ActorSystem, Props}

object App {

  def startRestService(conf:Config): Unit = {
    val actorSystem = ActorSystem("SQLServerActors")
    val tablesActor = actorSystem.actorOf(Props[TableRegistrationActor], name="TableRegistrationActor")
    if (conf.tableConfig.isDefined) tablesActor ! RegisterTables(Config.getTablesFromConfigFile(conf.tableConfig()))
    new RestService(conf.restPort(), tablesActor).startAndAwait()
  }

  def main(args : Array[String]) {
    val conf = new Config(args)
    SparkHelper.initContextAndHive(conf)
    startRestService(conf)
  }

}
