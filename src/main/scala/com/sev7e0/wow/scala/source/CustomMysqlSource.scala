package com.sev7e0.wow.scala.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

object CustomMysqlSource extends RichSourceFunction{


  override def open(parameters: Configuration): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[Nothing]): Unit = {

  }

  override def cancel(): Unit = {

  }
}
