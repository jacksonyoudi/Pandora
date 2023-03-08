package org.youdi.ch10

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.youdi.ch09.EventLog

object KeyStateDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.getConfig.setAutoWatermarkInterval(1000)

    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val source: DataStream[EventLog] = ds.map(
      a => {
        val words: Array[String] = a.split(" ")
        EventLog(words(0).toLong, words(1).toLong, words(2).toLong, words(3), words(0).toDouble)
      }
    )

    source.keyBy(_.user_id)
      .map(new RichMapFunction[EventLog, String] {
        var listState: ListState[String]


        override def open(parameters: Configuration) = {
          val context: RuntimeContext = getRuntimeContext
          listState = context.getListState(new ListStateDescriptor[String]("list", classOf[String]))
        }

        override def map(value: EventLog) = {
          listState.add(value.page)
          listState.toString
        }
      })



    env.execute(this.getClass.getName)
  }
}
