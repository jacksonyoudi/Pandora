package org.youdi.ch03

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SplitDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    //    val ds: DataStream[Long] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)


    val ss: DataStream[String] = ds.process(
      new ProcessFunction[String, String]() {
        override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]) = {
          if (value.startsWith("youdi")) {
            // 侧流
            ctx.output(OutputTag[String]("youdi"), value)
          } else {
            // 侧流
            ctx.output(OutputTag[String]("hello"), value)
          }

          // 主流
          out.collect(value)
        }
      }
    )

    val ys: DataStream[String] = ss.getSideOutput(OutputTag[String]("youdi"))
    val hs: DataStream[String] = ss.getSideOutput(OutputTag[String]("hello"))




    env.execute(this.getClass.getName)
  }

}
