package org.youdi.ch02

import org.apache.flink.streaming.api.datastream.IterativeStream
import org.apache.flink.streaming.api.scala._

object IterateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val strs: DataStream[String] = env.socketTextStream("local", 9998)
    //    val ds: DataStream[Long] = strs.map(_.toLong)
    // stepFunction: DataStream[T] => (DataStream[T], DataStream[R]),
    //                    maxWaitTimeMillis:Long = 0

    // stepfunction: initialStream => (feedback, output)
    strs.iterate(
      initialStream => {
        val fb: DataStream[String] = initialStream.filter(_ == "haha").setParallelism(1)
        fb.print()
        val output: DataStream[String] = initialStream.filter(_ != "haha")
        output
        (fb, output)
      }
    ).print()

    env.execute(this.getClass.getName)
  }
}
