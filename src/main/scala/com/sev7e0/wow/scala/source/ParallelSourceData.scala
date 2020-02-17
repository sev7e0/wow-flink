package com.sev7e0.wow.scala.source

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * Parallel Source Function class
 */
private [scala] class ParallelSourceData extends RichParallelSourceFunction[Long]{

  var number = 1L
  var running = true
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
