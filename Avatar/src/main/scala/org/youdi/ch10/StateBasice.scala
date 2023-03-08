package org.youdi.ch10

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.youdi.ch09.EventLog

object StateBasice {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    // 开启  task级别故障自动 failover
    // env.setRestartStrategy(RestartStrategies.noRestart());
    // 默认是，不会自动failover；一个task故障了，整个job就失败了
    // 使用的重启策略是： 固定重启上限和重启时间间隔
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))

    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val source: DataStream[EventLog] = ds.map(
      a => {
        val words: Array[String] = a.split(" ")
        EventLog(words(0).toLong, words(1).toLong, words(2).toLong, words(3), words(0).toDouble)
      }
    )


    env.execute(this.getClass.getName)
  }
}
