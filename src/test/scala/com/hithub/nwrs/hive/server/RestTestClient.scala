package com.hithub.nwrs.hive.server

import com.twitter.finagle.Http
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{RequestBuilder, Response}
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import net.liftweb.json.Serialization.write
import net.liftweb.json.{NoTypeHints, Serialization, parse}

class RestTestClient(root:String, secure:Boolean = false) {
  implicit val formats = Serialization.formats(NoTypeHints)
  val client =  ClientBuilder()
    .hosts(root)
    .hostConnectionLimit(10)
    .tcpConnectTimeout(Duration.fromSeconds(10))
    .stack(Http.client)
    .build()

  val requestRoot = (if (secure) "https://" else "http://")+root

  def getSync(path:String):Response = Await.result(client(RequestBuilder.create().url(requestRoot+path).buildGet()))

  def get(path:String):(Int,String) = {
    val response = getSync(path)
    (response.statusCode, response.getContentString())
  }

  def getAs[T](path:String)(implicit m: Manifest[T]):(Int,T) = {
    val response = Await.result(client(RequestBuilder.create().url(requestRoot+path).buildGet()))
    (response.statusCode, parse(response.getContentString()).extract[T])
  }

  def post[T](path:String, bodyJsonCaseClass:T)(implicit m: Manifest[T]):(Int) = {
    Await.result(client(RequestBuilder.create().url(requestRoot+path).buildPost(Buf.Utf8(write[T](bodyJsonCaseClass))))).statusCode
  }

  def delete(path:String):Int = Await.result(client(RequestBuilder.create().url(requestRoot+path).buildDelete())).statusCode
}

object RestTestClient {
  def success = 200
  def accepted:Int = 202
  def success[T](value:T) = (200, value)
  def notFound[T](value:T) = (404, value)
  def get(path:String)(implicit rest: RestTestClient):(Int,String) = rest.get(path)
  def getAs[T](path:String)(implicit m: Manifest[T], rest: RestTestClient):(Int,T) = rest.getAs[T](path)
  def post[T](path:String, bodyClass:T)(implicit m: Manifest[T], rest: RestTestClient):(Int) = rest.post[T](path, bodyClass)
  def delete(path:String)(implicit rest: RestTestClient):(Int) = rest.delete(path)
}
