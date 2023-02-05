package org.youdi.ch02

import org.apache.flink.api.common.{ExecutionConfig, ExecutionMode}
import org.apache.flink.streaming.api.scala._


/**
 * source类型
 * 1. 基于集合的
 * 2. 基于socket的
 * 3. 基于文件的
 * 4. 第三方的 connect source
 * 5. 自定义的source的
 *
 *
 * source又区分并行的， 和 非并行的
 *
 */
object SourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val cfg: ExecutionConfig = env.getConfig

    cfg.setExecutionMode(ExecutionMode.PIPELINED)
    cfg.setMaxParallelism(10)


    env.execute("source")

  }
}
