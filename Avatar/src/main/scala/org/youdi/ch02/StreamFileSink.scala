package org.youdi.ch02

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
 * sink不但可以将数据写入到各种文件系统，而且还整合了checkpoint机制来保证exacly once语义
 * 还可以对文件进行分桶存储，还可以支持以列式存储格式写入，功能强大
 *
 * 文件状态
 * 1. in-progress files
 * 2. Pending files
 * 3. finished files
 */

object StreamFileSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")
    env.setParallelism(2)

    val ds: DataStream[String] = env.addSource(MyParallelSource)

    FileSink.forRowFormat(new Path("hdfs://xxx"), new SimpleStringEncoder[String]("utf-8"))





    env.execute(this.getClass.getName)
  }
}
