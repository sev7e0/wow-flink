package com.sev7e0.wow.scala.source

import com.sev7e0.wow.utils.KafkaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 消费kafka中的消息。使用`com.sev7e0.wow.utils.KafkaUtils`产生数据。
 */
object KafkaSourceData {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource = environment.addSource(new FlinkKafkaConsumer011[String]("message",
      new SimpleStringSchema(),
      KafkaUtils.getProperty))

    kafkaSource.print()

    environment.execute(KafkaSourceData.getClass.getName)

  }
}
