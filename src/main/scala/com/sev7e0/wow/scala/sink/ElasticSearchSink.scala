package com.sev7e0.wow.scala.sink

import java.util

import com.sev7e0.wow.utils.KafkaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient.FailureListener
import org.elasticsearch.client.RestClientBuilder

object ElasticSearchSink {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val value = environment.addSource(new FlinkKafkaConsumer011[String]("message",
      new SimpleStringSchema(),
      KafkaUtils.getProperty)
    )

    val hosts = new util.ArrayList[HttpHost]
    hosts.add(new HttpHost( "spark01",9200,"http"))
    hosts.add(new HttpHost( "spark02",9200,"http"))
    hosts.add(new HttpHost( "spark03",9200,"http"))

    val builder = new ElasticsearchSink.Builder[String](hosts, new ElasticSearchSinkFunction)

    // 不使用批处理请求，将在每一条数据后进行写入，不会被缓存
    builder.setBulkFlushMaxActions(1)
    builder.setFailureHandler(new RetryRejectedExecutionFailureHandler())
    builder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        restClientBuilder.setMaxRetryTimeoutMillis(90000)
        restClientBuilder.setFailureListener(new FailureListener(){
          override def onFailure(host: HttpHost): Unit = {
            println("写入失败！"+ hosts)
          }
        })
      }
    })
    value.addSink(builder.build())

    environment.execute(ElasticSearchSink.getClass.getName)
  }


}
