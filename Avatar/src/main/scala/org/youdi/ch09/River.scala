package org.youdi.ch09

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration


object River {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val source: DataStream[EventLog] = ds.map(
      a => {
        val words: Array[String] = a.split(" ")
        EventLog(words(0).toLong, words(1).toLong, words(2).toLong, words(3), words(0).toDouble)
      }
    )

    val vm: DataStream[EventLog] = source.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withIdleness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[EventLog]() {
            override def extractTimestamp(element: EventLog, recordTimestamp: Long) = {
              element.ts
            }
          }
        )
    )

    val tag: OutputTag[EventLog] = new OutputTag[EventLog]("late_data")
    val dd: DataStream[String] = vm.keyBy(_.user_id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(tag)
      //      .sum("duration")
      .apply(
        // IN, OUT, KEY, W
        new WindowFunction[EventLog, String, Long, TimeWindow]() {
          override def apply(key: Long, window: TimeWindow, input: Iterable[EventLog], out: Collector[String]): Unit = {
            val size: Int = input.size
            out.collect("start:" + window.getStart + "," + "end:" + window.getEnd + " count:" + size)
          }
        }
      )

    // 延迟的数据
    val ls: DataStream[EventLog] = dd.getSideOutput(tag)

    dd.print("result")

    vm.keyBy(_.user_id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
      .trigger(new MyEventTimeTrigger)
      .evictor(new MyTimeEvictor(10))
      .apply(
        new WindowFunction[EventLog, String, Long, TimeWindow]() {
          override def apply(key: Long, window: TimeWindow, input: Iterable[EventLog], out: Collector[String]): Unit = {
            out.collect("window_start:" + window.getStart + "," + "window_end:" + window.getEnd + "," + input.size)
          }
        }
      )


    env.execute(this.getClass.getName)
  }
}
