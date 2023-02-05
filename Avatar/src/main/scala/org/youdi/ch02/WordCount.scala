package org.youdi.ch02

import org.apache.flink.api.common.{ExecutionConfig, ExecutionMode}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val config: ExecutionConfig = env.getConfig
    config.setExecutionMode(ExecutionMode.PIPELINED)


    val cfg: Configuration = new Configuration
    cfg.setInteger("res.port", 8081)
    val ev: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)


    val source: DataStream[String] = env.socketTextStream("localhost", 99999)


    val ds: DataStream[(String, Int)] = source.flatMap(
      _.toLowerCase.split("\\w+")
    )
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .window(
        TumblingProcessingTimeWindows.of(Time.seconds(5))
      ).sum(1)

    ds.print()

    env.execute("window Stream WordCount")


  }
}
