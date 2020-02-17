package com.sev7e0.wow.scala.basic

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * transformation operation practice of `flatmap` `map` `keyBy`
 */
object WindowWordCountScala {
  def main(args: Array[String]): Unit = {

    //通过flink提供的ParameterTool获取传入参数
    val hostname = ParameterTool.fromArgs(args).get("hostname")
    val port = ParameterTool.fromArgs(args).getInt("port")

    import org.apache.flink.api.scala._
    //获取执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //通过socket获取数据
    val words = environment.socketTextStream(hostname, port)

    //解析数据，分组处理，并聚合
    val wordCount = words.flatMap(line => line.split(" "))
      .map(w => WordCount(w,1))
      .keyBy(0)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1)

    wordCount.print().setParallelism(1)
    environment.execute(WindowWordCountScala.getClass.getName)
  }

  case class WordCount(word: String, count: Long)

}
