package org.youdi.ch02

import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object TextSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("local", 99989)
    val result: DataStream[(String, Long)] = ds.flatMap(
      (value: String, out: Collector[(String, Long)]) => {
        val words: Array[String] = value.split(" ")
        for (elem <- words) {
          out.collect((elem, 1L))
        }
      }).keyBy(0).sum(1)


    // 写入目录中， 文件名： 该sink所在的subtask的 index + 1
    result.writeAsText("hdfs://xxxx.txt", FileSystem.WriteMode.OVERWRITE)
    result.writeUsingOutputFormat(new TextOutputFormat(new Path("xxxx")))

    val path: String = ""

    // 本质上是使用csvOutputFormat格式写入
    // sink不是将数据实时写入文件中的，而是有一个bufferedOutputStream，默认缓存的大小为4096个字节，才会flush
    result.writeAsCsv(path)





    env.execute(this.getClass.getName)
  }

}
