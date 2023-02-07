package org.youdi.ch03

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * 以下函数都支持将数据输出到侧流
 * ProcessFunction
 * KeyedProcessFunction
 * CoProcessFunction
 * KeyedCoeProcessFunction
 * ProcessWindowFunction
 * ProcessAllWindowFunction
 */

object SideOutputDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)

    val one: OutputTag[String] = OutputTag[String]("sideOne")

    val all: DataStream[String] = ds.process(
      (value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]) => {
        if (value.startsWith("hello")) {
          ctx.output(one, value)
        }

        out.collect(value)
      }
    )

    val oneStream: DataStream[String] = all.getSideOutput(one)
    oneStream.print(this.getClass.getName)
    env.execute(this.getClass.getName)
  }
}
