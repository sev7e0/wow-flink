package com.sev7e0.wow.utils

object CustomMessageUtil {

  case class Message(sender: String, receiver: String, times: Long, content: String, tags: Map[String, Any])


  def produceMessage: Message = {
    var tags: Map[String, Any] = Map()
    tags += ("type" -> "custom")
    tags += ("status" -> "received")
    tags += ("delay" -> false)
    Message("sev7e0", "mario", System.currentTimeMillis(), "super mario Odyssey", tags)
  }


}
