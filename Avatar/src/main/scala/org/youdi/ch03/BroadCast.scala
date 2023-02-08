package org.youdi.ch03

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadCast {
  def main(args: Array[String]): Unit = {
    val cfg: Configuration = new Configuration
    cfg.setInteger("rest.port", 9998)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg)
    env.setParallelism(1)
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)
    val ss: DataStream[(String, String)] = ds.map(s => {
      val words: Array[String] = s.split(",")
      (words(0), words(1))
    })

    val ds1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val ss1: DataStream[(String, String, String)] = ds.map(s => {
      val words: Array[String] = s.split(",")
      (words(0), words(1), words(2))
    })

    val userInfoStateDesc: MapStateDescriptor[String, (String, String)] = new MapStateDescriptor("userInfoStateDesc", TypeInformation.of(classOf[String]), TypeInformation.of(new TypeHint[(String, String)] {}))

    val s2BroadCast: BroadcastStream[(String, String, String)] = ss1.broadcast(userInfoStateDesc)

    // 哪个流处理中需要用到广播状态数据，就要 去  连接 connect  这个广播流
    val sb: BroadcastConnectedStream[(String, String), (String, String, String)] = ss.connect(s2BroadCast)
    val result: DataStream[String] = sb.process(
      new BroadcastProcessFunction[(String, String), (String, String, String), String]() {
//        本方法，是用来处理 主流中的数据（每来一条，调用一次）
        override def processElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String), (String, String, String), String]#ReadOnlyContext, out: Collector[String]) = {
          val state: ReadOnlyBroadcastState[String, (String, String)] = ctx.getBroadcastState(userInfoStateDesc)
          if (state != null) {
            val userInfo: (String, String) = state.get(value._1)
            out.collect(
              value._1 + "," + value._2 + userInfo.toString
            )
          } else {
            out.collect(value._1 + "," + value._2 + "")
          }

        }

        override def processBroadcastElement(value: (String, String, String), ctx: BroadcastProcessFunction[(String, String), (String, String, String), String]#Context, out: Collector[String]) = {
          // 从上下文中，获取广播状态对象（可读可写的状态对象）
          val state: BroadcastState[String, (String, String)] = ctx.getBroadcastState(userInfoStateDesc)
          // 然后将获得的这条  广播流数据， 拆分后，装入广播状态
          state.put(value._1, (value._2, value._3))
        }
      }
    )

    result.print()

    env.execute(this.getClass.getName)
  }
}
