package org.youdi.ch02

import org.apache.flink.api.java.io.{CsvInputFormat, RowCsvInputFormat, TextInputFormat}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._


object FileDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path: String = "1.txt"

    // PROCESS_CONTINUOUSLY 一直监控文件变化
    val ds: DataStream[String] = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000)
//    val ds: DataStream[String] = env.readFile(new RowCsvInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000)

    val ds1: DataStream[String] = env.readTextFile(path)



    env.execute("File Source")
  }
}
