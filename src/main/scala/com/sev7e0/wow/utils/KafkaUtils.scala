package com.sev7e0.wow.utils

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



object KafkaUtils {

  //send message to kafka
  def main(args: Array[String]): Unit = {
    while(true){
      val record = new ProducerRecord[String, String]("message", CustomMessageUtil.produceMessage.toString)
      getProducer.send(record)
      Thread.sleep(2000)
    }
  }

  def getProperty: Properties = {
    getProperty(ParameterTool.fromSystemProperties())
  }

  def getProducer = new KafkaProducer[String, String](getProperty)

  def getConsumer = new KafkaConsumer[String, String](getProperty)

  def getProperty(parameterTool: ParameterTool): Properties= {
    val props = new Properties()
    props.put("bootstrap.servers",parameterTool.get("bootstrap.servers","spark01:9092"))
    props.put("zookeeper.connect", parameterTool.get("zookeeper.connect","spark01:2181"))
    props.put("group.id", parameterTool.get("group-id","message-group"))
    props.put("key.deserializer", parameterTool.get("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")) //key 反序列化
    props.put("key.serializer", parameterTool.get("key.serializer","org.apache.kafka.common.serialization.StringSerializer")) //key 反序列化
    props.put("value.deserializer", parameterTool.get("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"))
    props.put("value.serializer", parameterTool.get("value.serializer","org.apache.kafka.common.serialization.StringSerializer"))
    props.put("auto.offset.reset", parameterTool.get("auto.offset.reset","latest"))
    props
  }
}
