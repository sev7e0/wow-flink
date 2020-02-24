package com.sev7e0.wow.scala.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Objects._

import com.sev7e0.wow.utils.CustomMessageUtil.Message
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.parsing.json.JSON



object CustomMysqlSource extends RichSourceFunction[Message]{

  val url = "jdbc:mysql://localhost:3309/flink"
  val user = "root"
  val pwd = "mysql-docker"
  var statement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    val sql: String = "select * from message";
    val connection: Connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.prepareStatement(sql)
  }

  override def run(ctx: SourceFunction.SourceContext[Message]): Unit = {
    val resultSet:ResultSet = statement.executeQuery()
    while (resultSet.next()){
      val message = Message(resultSet.getString("sender"),
        resultSet.getString("receiver"),
        resultSet.getLong("times"),
        resultSet.getString("content"),
        JSON.parseFull(resultSet.getString("tags")).get.asInstanceOf[Map[String,Any]])
      ctx.collect(message)
    }
  }

  override def cancel(): Unit = {
    if (nonNull(statement)){
      statement.close()
    }
  }
}
