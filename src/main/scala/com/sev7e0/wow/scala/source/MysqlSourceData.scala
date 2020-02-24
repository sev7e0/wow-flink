package com.sev7e0.wow.scala.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MysqlSourceData {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

//    environment.registerTypeWithKryoSerializer[ProtobufSerializer](CustomMysqlSource.getClass,ProtobufSerializer)
    import org.apache.flink.api.scala._
    environment.addSource(CustomMysqlSource).print().setParallelism(1)

    environment.execute(MysqlSourceData.getClass.getName)

  }
}
