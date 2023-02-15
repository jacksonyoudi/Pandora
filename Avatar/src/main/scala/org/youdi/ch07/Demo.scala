package org.youdi.ch07

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction, TimestampAssigner}
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
import org.apache.flink.streaming.api.operators.{InternalTimer, InternalTimerServiceImpl, OneInputStreamOperator, StreamSource, StreamSourceContexts, Triggerable}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.runtime.partitioner.{ForwardPartitioner, StreamPartitioner}


/**
 * watemark就是在事件时间语义世界观中,用于单调递增向前推进时间的一种标记
 * 核心机制是在数据流中周期性地插入一种时间戳单调递增的特殊数据元素(watermark),来不可逆地在整个数据流中进行时间的推进
 *
 * watermark是从一个算子实例 源头 开始,根据数据中的事件时间,来周期性地产生,并插入到数据流中,
 * 持续不断地往下游传递,以推进整个计算链条上各算子的实例时间
 *
 *
 * watermark 本质上也是flink中各个算子间流转的一种标记数据, 只不过与用户的数据不同,它是flink内部自动产生并插入到数据流的
 * 它本身所携带的信息很简单,就是一个时间戳
 */
object Demo {
  def main(args: Array[String]): Unit = {
//    OneInputStreamOperator
//    StreamSource
//    AsyncWaitOperator
//    RichMapFunction
    ProcessFunction
    KeyedProcessFunction
    Triggerable
    SourceFunction
    StreamSourceContexts
    TwoPhaseCommitSinkFunction
    ListCheckpointed
    ChannelSelector
    StreamPartitioner
    ForwardPartitioner
    Trigger
    Evictor
    TimestampAssigner
    KeyedProcessFunction
    InternalTimer
    InternalTimerServiceImpl
    SlidingEventTimeWindows
  }
}
