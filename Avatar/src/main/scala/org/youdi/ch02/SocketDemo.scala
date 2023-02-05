package org.youdi.ch02

import org.apache.flink.streaming.api.scala._


object SocketDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("localhost", 99998, '\n', 2)
    ds.print()
    env.execute("socket")
  }
}
