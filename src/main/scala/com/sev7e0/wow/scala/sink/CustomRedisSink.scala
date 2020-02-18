package com.sev7e0.wow.scala.sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object CustomRedisSink {

  def main(args: Array[String]): Unit = {
    var host = "localhost"
    var port:Int = 9090
    try {
      host = ParameterTool.fromArgs(args).get("host", "localhost")
      port = ParameterTool.fromArgs(args).getInt("port", 9090)
    } catch {
      case _: Exception =>
        println("please use --host and --port")
    }
    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketSource = environment.socketTextStream(host, port).setParallelism(1)

    val mapSource = socketSource.map(source => {
      val strings = source.split(" ")
      Tuple2(strings(0), strings(1))
    })

    val jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    val redisSink = new RedisSink(jedisPoolConfig, new CustomRedisMapper)


    mapSource.addSink(redisSink)

    environment.execute(CustomRedisSink.getClass.getName)
  }
}