package org.youdi.ch02

import org.apache.flink.streaming.api.scala._

object FromElementDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.fromElements("flink", "hadoop", "flink")
    ds.print("word")
    env.execute("FromElement")
  }
}
