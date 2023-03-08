package org.youdi.ch10

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, OperatorStateStore}
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

  /*
  系统对状态数据做快照（持久化）时会调用的方法，用户利用这个方法，在持久化前，对状态数据做一些操控
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("checkpoint: checkout_id" + context.getCheckpointId)
  }


  /*
  算子任务在启动之初，会调用下面的方法，来为用户进行状态数据初始化
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    // 从方法提供的context中拿到一个算子状态存储器
    val store: OperatorStateStore = context.getOperatorStateStore

    // 算子状态存储器，只提供List数据结构来为用户存储数据
    val desc: ListStateDescriptor[String] = new ListStateDescriptor[String]("hello", classOf[String])

    // getListState方法，在task失败后，task自动重启时，会帮用户自动加载最近一次的快照状态数据
    // 如果是job重启，则不会自动加载此前的快照状态数据

    // 在状态存储器上调用get方法，得到具体结构的状态管理器
    val listState: ListState[String] = store.getListState(desc)


    /**
     * unionListState 和普通 ListState的区别：
     * unionListState的快照存储数据，在系统重启后，list数据的重分配模式为： 广播模式； 在每个subtask上都拥有一份完整的数据
     * ListState的快照存储数据，在系统重启后，list数据的重分配模式为： round-robin； 轮询平均分配
     */
    //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);

  }
}
