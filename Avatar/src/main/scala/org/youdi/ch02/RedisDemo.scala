package org.youdi.ch02
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig



object RedisDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration()
    cfg.setInteger("rest.port", 8822)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://ddd")

    val ds: DataStream[String] = env.socketTextStream("localhost", 10999)
//    FlinkJedisPoolConfig.Builder.

    env.execute(this.getClass.getName)
  }
}
