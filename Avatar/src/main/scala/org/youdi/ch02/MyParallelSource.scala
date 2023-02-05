package org.youdi.ch02

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}


// flink的data stream API可以让开发者根据实际需要，灵活的自定义source，本质上就是定义一个类
// 实现sourceFunction 或继承RichParallelSourceFunction,实现run方法和cancel方法

/*
  1. function


  2. richfunction
        open
        close
        RuntimeContext


   自定义source
   可以实现sourceFunction 或者RichSourceFunction, 这两个都是非并行的source算子
   也可以实现 ParallelSourceFunction 或者 RichParallelSourceFunction,这两者都是可以并行的source算子

   rich , 都拥有 open, close, getRuntimeContext 方法
   带parallel的， 都可实例并行执行
 */
object MyParallelSource extends RichParallelSourceFunction[String] {
  private var i: Long = 1;
  private var flag: Boolean = true;

  //  run方法就是用来读取外部的数据或产生数据逻辑
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (i <= 10 && flag) {
      Thread.sleep(1000);
      i += 1
      ctx.collect("data: " + i)
    }
  }

  // cancel方法就是让source停止
  override def cancel(): Unit = {
    flag = false
  }
}
