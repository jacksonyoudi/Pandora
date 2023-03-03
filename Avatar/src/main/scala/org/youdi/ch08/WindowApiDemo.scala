package org.youdi.ch08

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.Duration

object WindowApiDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val source: DataStream[String] = env.socketTextStream("localhost", 9998)
    val ds: DataStream[EventLog] = source.map(
      a => {
        val strings: Array[String] = a.split(",")
        EventLog(strings(0).toLong, strings(1).toLong, strings(2).toLong, strings(3), strings(4).toDouble)
      }
    )

    val wmDS: DataStream[EventLog] = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofMillis(100))[EventLog]
        .withTimestampAssigner(
          new SerializableTimestampAssigner[EventLog]() {
            override def extractTimestamp(element: EventLog, recordTimestamp: Long) = {
              element.ts
            }
          }
        )
    )

    val sds: WindowedStream[EventLog, Long, TimeWindow] = wmDS
      .keyBy(_.user_id)
      .window(
        SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))
      )
      .allowedLateness(Time.seconds(20))


    sds.aggregate(
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


    //    需求 二 ：  每隔10s，统计最近 30s 的数据中，每个用户的平均每次行为时长
    sds.aggregate(
      new AggregateFunction[EventLog, (Double, Long), Double]() {
        override def createAccumulator() = {
          (0.toDouble, 0.toLong)
        }

        override def add(value: EventLog, accumulator: (Double, Long)) = {
          (accumulator._1 + value.Duration, accumulator._2 + 1)
        }

        override def getResult(accumulator: (Double, Long)) = {
          accumulator._1 / accumulator._2
        }

        override def merge(a: (Double, Long), b: (Double, Long)) = {
          (a._1 + b._1, a._2 + b._2)

        }
      }
    )

    env.execute(this.getClass.getName)

  }

}
