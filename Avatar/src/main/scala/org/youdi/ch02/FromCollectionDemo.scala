package org.youdi.ch02

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.{NumberSequenceIterator, SplittableIterator}

import java.lang

object FromCollectionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val list: List[String] = List("flink", "spark", "hadoop", "flink")
    //    env.fromParallelCollection(new SplittableIterator{})
    //  是一个并行的source，并行度可以使用env的setParallelism设置，
    // 第一个是继承SplittableIterator的实现类的迭代器，第二个是迭代器中的类型
    val ds1: DataStream[lang.Long] = env.fromParallelCollection(new NumberSequenceIterator(1L, 10L))


    //    val ds: DataStream[String] = env.fromCollection(list)


    ds1.print()

    env.execute("FromCollectionDemo")
  }

}
