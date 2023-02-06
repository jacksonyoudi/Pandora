package org.youdi.ch02

import org.apache.flink.streaming.api.scala._

object KeyByDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)

    ds.map(
      (line: String) => {
        val arr: Array[String] = line.split(" ")
        User(arr(0).toLong, arr(1), arr(2))
      }
    ).keyBy(_.address)


    env.execute("keyby demo")
  }
}
