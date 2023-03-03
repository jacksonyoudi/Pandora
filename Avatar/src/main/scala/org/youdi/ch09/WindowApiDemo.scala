package org.youdi.ch09

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.sql.Connection
import java.time.Duration
import scala.collection.mutable.HashMap


object WindowApiDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val source: DataStream[EventLog] = ds.map(
      a => {
        val words: Array[String] = a.split(" ")
        EventLog(words(0).toLong, words(1).toLong, words(2).toLong, words(3), words(0).toDouble)
      }
    )

    val wm: DataStream[EventLog] = source.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofMillis(100))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[EventLog]() {
            override def extractTimestamp(element: EventLog, recordTimestamp: Long) = {
              element.ts
            }
          }
        ).withIdleness(Duration.ofMillis(100))
    )

    val ks: KeyedStream[EventLog, Long] = wm.keyBy(_.user_id)

    val ws: WindowedStream[EventLog, Long, TimeWindow] = ks.window(
      SlidingEventTimeWindows
        .of(Time.seconds(30), Time.seconds(10))
    )


    // 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
    ws.aggregate(
      new AggregateFunction[EventLog, Long, Long]() {
        override def createAccumulator() = {
          0
        }

        override def add(value: EventLog, accumulator: Long) = {
          accumulator + 1
        }

        override def getResult(accumulator: Long) = {
          accumulator
        }

        override def merge(a: Long, b: Long) = {
          a + b
        }
      }
    )


    //  每隔10s，统计最近 30s 的数据中，每个用户的平均每次行为时长
    ws.aggregate(
      new AggregateFunction[EventLog, (Double, Long), Double]() {
        override def createAccumulator() = {
          (0.toDouble, 0.toLong)
        }

        override def add(value: EventLog, accumulator: (Double, Long)) = {
          (value.duration + accumulator._1, accumulator._2 + 1)
        }

        override def getResult(accumulator: (Double, Long)) = {
          accumulator._1 / accumulator._2
        }

        override def merge(a: (Double, Long), b: (Double, Long)) = {
          (a._1 + b._1, a._2 + b._2)
        }
      }
    )

    // 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
    ws.sum("user_id")

    // 每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长
    ws.max("duration")

    // 每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长及其所在的那条行为记录
    ws.maxBy("duration")

    // 每隔10s，统计最近 30s 的数据中，每个页面上发生的行为中，平均时长最大的前2种事件及其平均时长
    wm.keyBy(_.page)
      .window(
        SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))
      )
//      .process(
//        // IN, OUT, KEY, W
////        new ProcessWindowFunction[]() {
////
////        }
//      )


    env.execute(this.getClass.getName)
  }
}
