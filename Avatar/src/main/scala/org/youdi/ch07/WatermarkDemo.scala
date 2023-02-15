package org.youdi.ch07

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

object WatermarkDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    new Watermark()


    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)


    env.execute(this.getClass.getName)
  }
}
