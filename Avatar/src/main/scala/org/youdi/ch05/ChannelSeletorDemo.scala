package org.youdi.ch05


import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ChannelSeletorDemo {

  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)

    val ms: DataStream[String] = ds
      .map(_.toUpperCase)
      .setParallelism(4)
      .flatMap(
        new FlatMapFunction[String, String]() {
          override def flatMap(value: String, out: Collector[String]) = {
            val strings: Array[String] = value.split(" ")
            for (elem <- strings) {
              out.collect(elem)
            }
          }
        }
      )
      .setParallelism(4)
      .forward


    ms.map(_.toLowerCase).setParallelism(4).keyBy(_.substring(0, 2))
      .process(
        new KeyedProcessFunction[String, String, String]() {
          override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]) = {
            out.collect(value + " <>")
          }
        }
      ).setParallelism(5).broadcast.rebalance.rescale.print


    env.execute(this.getClass.getName)
  }
}
