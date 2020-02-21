package com.sev7e0.wow.scala.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TextFileData {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val textSource = environment.readTextFile("src/main/resources/kv1.txt")

//    val flatSource = textSource.flatMap(source => source.split("_"))

    textSource.print().setParallelism(1)

    environment.execute(TextFileData.getClass.getName)

  }
}
