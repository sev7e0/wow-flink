package com.sev7e0.wow.scala.transformation
import com.sev7e0.wow.scala.source.SingleSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TransformationsWithSelectAndSplit {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.addSource(new SingleSourceScala).setParallelism(1)

    val splitSource = source.split(value => {
      var list = List.empty
      if (value % 2 == 0) {
//        list.++("even"); //偶数
      } else {
//        list.+:("odd"); //奇数
      }
      list
    })

    val evenStream = splitSource.select("even")
    val oddStream = splitSource.select("odd")
    val moreStream = splitSource.select("odd", "even")

    evenStream.print()
    oddStream.print()
    moreStream.print()

    environment.execute(TransformationsWithSelectAndSplit.getClass.getName)

  }
}
