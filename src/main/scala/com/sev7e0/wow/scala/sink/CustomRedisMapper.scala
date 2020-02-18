package com.sev7e0.wow.scala.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class CustomRedisMapper extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.LPUSH)
  }

  override def getKeyFromData(t: (String, String)): String = {
    t._1
  }

  override def getValueFromData(t: (String, String)): String = {
    t._2
  }
}
