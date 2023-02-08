package org.youdi.ch03

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

    val ss: DataStream[String] = ds.map(
      new CoMapFunction[String, Int, String]() {
        override def map1(value: String) = {
          value.toUpperCase()
        }

        override def map2(value: Int) = {
          value.toString
        }
      }
    )


    ds.flatMap(
      new CoFlatMapFunction[String, Int, String] {
        override def flatMap1(value: String, out: Collector[String]) = {
          val arr: Array[String] = value.split(" ")
          for (elem <- arr) {
            out.collect(elem)
          }
        }

        override def flatMap2(value: Int, out: Collector[String]) = {
          out.collect(value.toString)
        }
      }
    )


    env.execute(this.getClass.getName)
  }

}
