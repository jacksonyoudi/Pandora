package org.youdi.ch04


import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object   ProcessFunctionD {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    ds.process(
      new ProcessFunction[String, (String, String)]() {


        // 通过getRuntimeContext获取上下文信息
        override def open(parameters: Configuration) = {
          val context: RuntimeContext = getRuntimeContext
          context.getTaskName
          super.open(parameters)
        }

        override def processElement(value: String, ctx: ProcessFunction[String, (String, String)]#Context, out: Collector[(String, String)]) = {
          ctx.output(new OutputTag[String]("hello"), value)
          val arr: Array[String] = value.split(" ")
          out.collect((arr(0), arr(1)))
        }

        override def close() = {
          super.close()
        }
      }


    )


    env.execute(this.getClass.getName)
  }

}
