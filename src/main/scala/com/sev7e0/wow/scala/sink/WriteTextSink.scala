package com.sev7e0.wow.scala.sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Write to text file
 */
object WriteTextSink {

  def main(args: Array[String]): Unit = {

    var host = ""
    var port:Int = 0
    try {
      host = ParameterTool.fromArgs(args).get("host", "localhost")
      port = ParameterTool.fromArgs(args).getInt("port", 9090)
    } catch {
      case _: Exception =>
        println("please use --host and --port")
    }
    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.socketTextStream(host, port)

    val value = source.flatMap(str => str.split(" "))

    value.writeAsText("target")

    environment.execute(WriteTextSink.getClass.getName)
  }
  case class WordCount(word: String, count:Int = 1)
}
