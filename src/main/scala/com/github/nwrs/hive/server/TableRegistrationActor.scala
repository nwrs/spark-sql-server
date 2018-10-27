package com.github.nwrs.hive.server

import akka.actor.Actor
import scala.collection.immutable.TreeMap
import scala.collection.mutable

sealed trait Command
case class RegisterTable(table:String, file:String) extends Command
case class RegisterTables(tables:Map[String,String]) extends Command
case class DeRegisterTable(table:String) extends Command
object GetRegisteredTables extends Command

class TableRegistrationActor extends Actor {

  private[this] val registered = mutable.Map[String,String]()

  private[this] def register(table:String, file:String) = {
    deregister(table)
    SparkHelper.registerTable(table, file)
    registered += (table -> file)
  }

  private[this] def deregister(table:String):Unit = if (registered.remove(table).isDefined) SparkHelper.deregisterTable(table)

  override def receive: Receive = {
    case rt:RegisterTable => register(rt.table, rt.file)
    case rt:RegisterTables => rt.tables.foreach( t => register(t._1, t._2))
    case drt:DeRegisterTable => deregister(drt.table)
    case GetRegisteredTables => sender ! TreeMap(registered.toSeq :_*)
  }

}
