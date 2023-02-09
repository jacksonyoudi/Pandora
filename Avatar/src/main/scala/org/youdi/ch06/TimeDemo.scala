package org.youdi.ch06

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object TimeDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    // 如果设置0 ，表示禁用
    env.getConfig.setAutoWatermarkInterval(1000)



    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val ss: KeyedStream[String, String] = ds.keyBy(_.substring(1, 2))
    ss.window(
      SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
    )

    ss.window(
      SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1))
    )

    ss.window(TumblingEventTimeWindows.of(Time.seconds(4)))

    ss.window(TumblingProcessingTimeWindows.of(Time.seconds(4)))



    // 构造 watermarker的生成策略 算法策略，以及事件的抽取方法





    env.execute(this.getClass.getName)
  }
}
