package com.sev7e0.wow.scala.transformation

import com.sev7e0.wow.scala.source.SingleSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * transformation of connect conMap conFlatMap
 */
object TransformationWithConnect {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val source_1 = env.addSource(new SingleSourceScala).setParallelism(1)
    val source_2 = env.addSource(new SingleSourceScala).setParallelism(1)
    val source_21 = source_2.map(source_21 => "source_2_" + source_21)
    val conSource = source_1.connect(source_21)

    val conMapSource = conSource.map(value1 => value1.toString, value2 => value2)

    conMapSource.print().setParallelism(1)

    env.execute(TransformationsWithSelectAndSplit.getClass.getName)
  }
}
