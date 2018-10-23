package com.github.nwrs.hive.server

import java.util.concurrent.TimeUnit
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Http, Service}
import io.finch.Ok
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import io.finch._
import io.finch.syntax._
import net.liftweb.json.Serialization.{read, write}
import net.liftweb.json._
import org.slf4j.LoggerFactory
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

case class Tables(tables:Map[String,String])
case class RegisterTableRqt(table:String, file:String)
case class TableRqt(table:String)

class RestService(port:Int, tableRegistrationActor: ActorRef) {
  val log = LoggerFactory.getLogger(this.getClass)
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val timeout:Timeout = Timeout(30, TimeUnit.SECONDS)

  //TODO Endpoints should use implicit Finch Json encoding and be of type Application.JSON where relevant etc..

  /**
    * POST to /table with a JSON body as per RegisterTableRqt to register a new table
    *
    */
  val registerTable: Endpoint[Unit] = post("table" :: stringBody) { body:String=>
    try {
      val rqt = read[RegisterTableRqt](body)
      log.info("POST to '/table' to register table.")
      tableRegistrationActor ! RegisterTable(rqt.table, rqt.file)
      Accepted[Unit]
    } catch {
      case e:Exception =>
        log.error(s"POST '/table' register table failed. $e")
        InternalServerError(e)
    }
  }

  /**
    * DELETE /table/{tableName} to de-register (drop) a table
    *
    */
  val deregisterTable: Endpoint[Unit] = delete("table" :: path[String]) { tableName:String =>
    try {
      log.info(s"DELETE to '/table/$tableName' to de-register table.")
      tableRegistrationActor ! DeRegisterTable(tableName)
      Accepted[Unit]
    } catch {
      case e:Exception =>
        log.error(s"DELETE '/table/$tableName' failed. $e")
        InternalServerError(e)
    }
  }

  /**
    * GET /tables to retrieve list of all available registered tables and their associated parquet files
    */
  val tables: Endpoint[String] = get("tables") {
    try {
      log.info(s"GET '/tables' to retrieve all registered tables.")
      val res = tableRegistrationActor ? GetRegisteredTables
      val tables = Await.result(res, Duration(30, TimeUnit.SECONDS)).asInstanceOf[Map[String,String]]
      Ok(write(Tables(tables)))
    } catch {
      case e:Exception => {
        log.error(s"GET '/tables' failed. $e")
        InternalServerError(e)
      }
    }
  }

  /**
    * GET /table/{tableName} to check status of a table
    */
  val table: Endpoint[String] = get("table" :: path[String]) { tableName:String =>
    try {
      log.info(s"GET '/table/$tableName' to retrieve table info.")
      val res = tableRegistrationActor ? GetRegisteredTables
      val tables = Await.result(res, Duration(30, TimeUnit.SECONDS)).asInstanceOf[Map[String,String]]
      if (tables.contains(tableName)) {
        Ok(write(Tables(Map((tableName -> tables.get(tableName).get)))))
      } else {
        NotFound(new Exception(s"Table '$tableName' not found"))
      }
    } catch {
      case e:Exception => {
        log.error(s"GET /table/$tableName failed. $e")
        InternalServerError(e)
      }
    }
  }

  def startAndAwait():Unit = {
    import com.twitter.util.Await
    val api: Service[Request, Response] = (table :+: tables :+: registerTable :+: deregisterTable).toServiceAs[Text.Plain]
    log.info(s"Creating REST endpoint on port $port")
    Await.ready(Http.server.serve(s":$port", api))
  }

}
