package org.youdi.ch09

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import scala.collection.mutable
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
      .process(
        // IN, OUT, KEY, W
        new ProcessWindowFunction[EventLog, (String, Double, Long), String, TimeWindow]() {
          override def process(key: String, context: Context, elements: Iterable[EventLog], out: Collector[(String, Double, Long)]): Unit = {
            val map: HashMap[String, (Double, Long)] = new HashMap[String, (Double, Long)]()

            for (elem <- elements) {
              val page: String = elem.page
              val t: (Double, Long) = map.getOrElse(page, (0.toDouble, 0L))
              map.put(page, (t._1 + elem.duration, t._2 + 1L))
            }

            def max_duration(x: (String, Double, Long)) = x._2 / x._3.toDouble;

            //  函数柯里化
            val queue: mutable.PriorityQueue[(String, Double, Long)] = new mutable.PriorityQueue[(String, Double, Long)]()(Ordering.by(max_duration))
            queue.sizeHint(2)

            for (elem <- map) {
              queue.enqueue((elem._1, elem._2._1, elem._2._2))
            }


            // 判断大小
            for (elem <- 1 to 2) {
              if (!queue.isEmpty) {
                out.collect(queue.dequeue())
              }
            }
          }
        }

      )

    // 每隔10s，统计最近 30s 的数据中，每个用户的行为事件中，行为时长最长的前2条记录
    wm.keyBy(_.page)
      .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))

      // 窗口函数
      .apply(
        // IN, OUT, KEY, W
        new WindowFunction[EventLog, (Double, Long), String, TimeWindow]() {
          override def apply(key: String, window: TimeWindow, input: Iterable[EventLog], out: Collector[(Double, Long)]): Unit = {
            def maxEvent(e: EventLog): Double = e.duration

            val queue = new mutable.PriorityQueue[EventLog]()(Ordering.by(maxEvent))

            for (elem <- input) {
              queue.enqueue(elem)
            }

            // 判断大小
            for (elem <- 1 to 2) {
              if (!queue.isEmpty) {
                val log: EventLog = queue.dequeue()
                out.collect((log.duration, log.id))
              }
            }

          }
        }
      )


    env.execute(this.getClass.getName)
  }
}
