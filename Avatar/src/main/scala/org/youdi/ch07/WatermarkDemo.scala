package org.youdi.ch07

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import java.time.Duration

object WatermarkDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)


    // WatermarkStrategy.noWatermarks()
    // forMonotonousTimestamps()
    // WatermarkStrategy.forBoundedOutOfOrderness()

    val watermarkerStrategy: WatermarkStrategy[String] = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofMillis(1000)) // 允许乱序的算法策略
      .withTimestampAssigner( //   提取 时间戳
        new SerializableTimestampAssigner[String]() {
          override def extractTimestamp(element: String, recordTimestamp: Long) = {
            val strings: Array[String] = element.split(",")
            strings(0).toLong
          }
        }
      )

    //    ds.assignTimestampsAndWatermarks(watermarkerStrategy)


    ds.map(_.toUpperCase)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofMillis(100))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[String]() {
              override def extractTimestamp(element: String, recordTimestamp: Long) =
                element.toLong
            }
          )

      )


    env.execute(this.getClass.getName)
  }
}
