package org.youdi.ch02

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object KeyByDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)


    val ks: KeyedStream[User, String] = ds.map(
      (line: String) => {
        val arr: Array[String] = line.split(" ")
        User(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }
    ).keyBy(_.address)

    //  sum可以是  index位置 或者 对应bean的字段名
    ks.sum("cnt")

    ks.minBy(2)

    //  min, minBy
    // min的返回值，最小值字段外，其他字段是第一条输入数据的值
    // minby返回值, 就是最小值字段所在的那条数据
    // 底层原理： 滚动更新时，是更新一个字段，还是更新整条数据的区别


    // rueduce是将
    ks.reduce(
      new ReduceFunction[User]() {
        override def reduce(value1: User, value2: User) = {
          value1.cnt += value2.cnt
          value1
        }
      }
    )

    ks.reduce(
      (value1: User, value2: User) => {
        value1.cnt += value2.cnt
        value1
      }
    )


    env.execute("keyby demo")
  }
}
