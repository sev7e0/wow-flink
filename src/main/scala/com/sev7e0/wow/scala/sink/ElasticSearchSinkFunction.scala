package com.sev7e0.wow.scala.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.client.Requests


class ElasticSearchSinkFunction extends ElasticsearchSinkFunction[String] {
  override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val map = new util.HashMap[String, String]()
    map.put("data", t)
    val request = Requests.indexRequest
      .index("flink")
      .`type`("flink-type")
      .source(map)
    requestIndexer.add(request)
  }
}
