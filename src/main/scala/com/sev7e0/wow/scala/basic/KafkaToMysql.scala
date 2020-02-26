package com.sev7e0.wow.scala.basic

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sev7e0.wow.scala.sink.CustomMysqlSink
import com.sev7e0.wow.utils.CustomMessageUtil.Message
import com.sev7e0.wow.utils.{CustomMessageUtil, KafkaUtils}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
 * Kafka to Mysql
 */
object KafkaToMysql {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val value1: DataStream[Message] =
    environment.addSource(new FlinkKafkaConsumer011[String]("message",
      new SimpleStringSchema(),
      KafkaUtils.getProperty)
    ).map(value => JSON.parseObject(value, classOf[Message]))

    value1.addSink(new CustomMysqlSink)

    environment.execute(KafkaToMysql.getClass.getName)
  }

}
