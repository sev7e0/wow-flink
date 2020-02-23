package com.sev7e0.wow.utils

import scala.util.parsing.json.{JSON, JSONObject}

object CustomMessageUtil {

  case class Message(sender: String, receiver: String, times: Long, content: String, tags: Map[String, Any])


  def produceMessage: Message = {
    var tags: Map[String, Any] = Map()
    tags += ("type" -> "custom")
    tags += ("status" -> "received")
    tags += ("delay" -> false)
    Message("sev7e0", "mario", System.currentTimeMillis(), "super mario Odyssey", tags)
  }

  def main(args: Array[String]): Unit = {
    println(JSONObject(produceMessage.tags))
    //map转json
    val tags = JSONObject(produceMessage.tags).toString()
    //json转map
    val option = JSON.parseFull(tags).get
    println(option)
  }

}
