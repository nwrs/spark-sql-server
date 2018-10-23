package com.github.nwrs.hive.server

import akka.actor.{ActorSystem, Props}

object App {

  def main(args : Array[String]) {

    val conf = new Config(args)
    Spark.initContextAndHive(conf)

    val actorSystem = ActorSystem("SQLServerActors")
    val tablesActor = actorSystem.actorOf(Props[TableRegistrationActor], name="TableRegistrationActor")

    if (conf.tableConfig.isDefined) tablesActor ! RegisterTables(Config.getTablesFromConfigFile(conf.tableConfig()))

    new RestService(conf.restPort(), tablesActor).startAndAwait()
  }

}
