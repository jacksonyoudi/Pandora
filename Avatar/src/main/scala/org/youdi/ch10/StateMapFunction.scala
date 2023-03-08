package org.youdi.ch10

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

import java.lang
import scala.collection.mutable


/*
 * 要使用operator state，需要让用户自己的Function类去实现 CheckpointedFunction
 * 然后在其中的 方法initializeState 中，去拿到operator state 存储器
 */
class StateMapFunction extends MapFunction[String, String] with CheckpointedFunction {
  var listState: ListState[String]

  override def map(value: String): String = {
    if (value == "x") {
      throw new Exception("xxx")
    }

    listState.add(value)

    //  然后把历史数据
    val value: lang.Iterable[String] = listState.get()
    val builder: mutable.StringBuilder = new StringBuilder()

    value.forEach(
      a => {
        builder.append(a)
      }
    )

    builder.toString()

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {}
}
