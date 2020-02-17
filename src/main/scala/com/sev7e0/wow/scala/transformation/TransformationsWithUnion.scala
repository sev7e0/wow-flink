package com.sev7e0.wow.scala.transformation

import com.sev7e0.wow.scala.source.SingleSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TransformationsWithUnion {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source_1 = environment.addSource(new SingleSourceScala)
    val source_2 = environment.addSource(new SingleSourceScala)

    val mapSource = source_1.union(source_2).map(source => {
      println("receive data source: " + source)
      source
    })

    val filterSource = mapSource.filter(ms => {
      ms % 2 == 0
    })

    filterSource.print().setParallelism(1)
    environment.execute(TransformationsWithUnion.getClass.getName)
  }
}
