package org.youdi.ch03

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object ConnectStreamDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val one: DataStream[String] = env.socketTextStream("localhost", 9998)
    val two: DataStream[Int] = env.fromElements(1 to 10: _*)

    val ds: ConnectedStreams[String, Int] = one.connect(two)

    env.execute(this.getClass.getName)
  }

}
