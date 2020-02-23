package com.sev7e0.wow.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



object KafkaUtils {

  //send message to kafka
  def main(args: Array[String]): Unit = {
    while(true){
      val record = new ProducerRecord[String, String]("message", CustomMessageUtil.produceMessage.toString)
      getProducer(getProperty).send(record)
      Thread.sleep(2000)
    }
  }

  def getProducer(properties: Properties) = new KafkaProducer[String, String](properties)

  def getConsumer(properties: Properties) = new KafkaConsumer[String, String](properties)

  def getProperty: Properties= {
    val props = new Properties()
    props.put("bootstrap.servers","spark01:9092")
    props.put("zookeeper.connect", "spark01:2181")
    props.put("group.id", "message-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("auto.offset.reset", "latest")
    props
  }
}
