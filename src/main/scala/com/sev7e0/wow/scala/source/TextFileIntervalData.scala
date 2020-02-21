package com.sev7e0.wow.scala.source

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TextFileIntervalData {

  def main(args: Array[String]): Unit = {

    val filePath = "src/main/resources/kv2.txt"
    val interval = 1000

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val path = new Path(filePath)
    val format = new TextInputFormat(path)
    val textSource = environment.readFile(format,
      filePath,
      // 如果 watchType 设置为 FileProcessingMode.PROCESS_CONTINUOUSLY，则当文件被修改时，
      // 其内容将被重新处理。这会打破“exactly-once”语义，因为在文件末尾附加数据将导致其所有内容被重新处理。
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      interval)

//    val flatSource = textSource.flatMap(source => source.split("_"))

    textSource.print().setParallelism(1)

    environment.execute(TextFileData.getClass.getName)

  }
}
