package com.sev7e0.wow.scala.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Objects

import com.sev7e0.wow.utils.CustomMessageUtil.Message
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.util.parsing.json.JSONObject

/**
 * 自定义MysqlSink
 */
class CustomMysqlSink extends RichSinkFunction[Message]{
  override def invoke(value: Message, context: SinkFunction.Context[_]): Unit = {
    statement.setString(1,value.sender)
    statement.setString(2,value.receiver)
    statement.setLong(3,value.times)
    statement.setString(4,value.receiver)
    statement.setString(5,JSONObject(value.tags).toString())
    statement.executeUpdate()
  }

  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext


  val url = "jdbc:mysql://localhost:3309/flink"
  val user = "root"
  val pwd = "mysql-docker"
  var statement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    val sql: String = "insert into message (`sender`,`receiver`,`times`,`content`,`tags`) VALUES (?,?,?,?,?)";
    val connection: Connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.prepareStatement(sql)
  }

  override def close(): Unit = {
    if (!Objects.isNull(statement)){
      statement.close()
    }
  }
}
