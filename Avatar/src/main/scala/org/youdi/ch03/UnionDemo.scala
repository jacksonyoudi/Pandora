package org.youdi.ch03

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang


object UnionDemo {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val one: DataStream[String] = env.socketTextStream("localhost", 9998)
    val two: DataStream[String] = env.socketTextStream("localhost", 9999)
    val all: DataStream[String] = one.union(two)

    val mm: DataStream[String] = one.coGroup(two).where(_.trim).equalTo(_.trim).window(
      TumblingEventTimeWindows.of(Time.seconds(5))
    ).apply(
      new CoGroupFunction[String, String, String]() {
        override def coGroup(first: lang.Iterable[String], second: lang.Iterable[String], out: Collector[String]) = {
          val buffer: StringBuffer = new StringBuffer()
          first.forEach(e => buffer.append(e))
          second.forEach(e => buffer.append(e))
          out.collect(buffer.toString)
        }
      }
    )

    mm.print()
    env.execute(this.getClass.getName)
  }
}
