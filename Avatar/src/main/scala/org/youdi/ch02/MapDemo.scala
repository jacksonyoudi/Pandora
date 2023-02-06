package org.youdi.ch02

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * MapFunction 实现map函数即可
 * RichMapFunction:
 * open close getRuntimeContext
 */

object MapDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = env.fromElements("hadoop", "spark", "flink", "hbase", "spark")
    val ds: DataStream[String] = words.map(
      _.toUpperCase
      //      new MapFunction[String, String] {
      //        override def map(t: String) = {
      //          t.toUpperCase()
      //        }
      //      }
    )

    ds.print()


    val dd: DataStream[(String, Int)] = words.flatMap(
      new FlatMapFunction[String, (String, Int)]() {
        override def flatMap(t: String, collector: Collector[(String, Int)]) = {
          val arr: Array[String] = t.split(" ")
          for (elem <- arr) {
            collector.collect((elem.toUpperCase, 1))
          }
        }
      }
    )


    // 使用lamada表达式
    val df1: DataStream[(String, Int)] = words.flatMap(
      (t: String, collector: Collector[(String, Int)]) => {
        val arr: Array[String] = t.split(" ")
        for (elem <- arr) {
          collector.collect((elem.toUpperCase, 1))
        }
      }
    )

        env.execute("Map Demo")
      }
  }
