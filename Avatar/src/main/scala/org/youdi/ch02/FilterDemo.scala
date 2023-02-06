package org.youdi.ch02

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.RichFilterFunction

import org.apache.flink.streaming.api.scala._

object FilterDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val dd: DataStream[Int] = ds.filter(
      new FilterFunction[Int]() {
        override def filter(value: Int) = {
          value % 2 == 0
        }
      }
    )

    val d2: DataStream[Int] = ds.filter(
      _ % 2 == 0
      //      (i:Int) => {i % 2 == 0}
    )

//    ds.filter(new RichFilterFunction[Int] {
//      override def filter(value: Int) = {
//        this.getRuntimeContext
//      }
//    })


    env.execute("filter demo")
  }
}
