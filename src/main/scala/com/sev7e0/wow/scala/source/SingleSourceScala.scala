package com.sev7e0.wow.scala.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}


private[scala] class SingleSourceScala extends RichSourceFunction[Long]{
  var number = 1L
  var running = true


  // execute in TaskManager.doRun method
  override def open(parameters: Configuration): Unit = {
    println("开始获取数据源预处理！")
  }


  override def close(): Unit = {
    println("关闭数据源！")
  }

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (running){
      sourceContext.collect(number)
      number += 1
      Thread.sleep(3000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
