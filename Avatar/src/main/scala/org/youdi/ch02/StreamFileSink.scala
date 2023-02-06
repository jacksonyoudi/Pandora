package org.youdi.ch02

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.ParquetWriterFactory
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
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

    val fileSink: FileSink[String] = FileSink
      //  文件格式
      .forRowFormat(new Path("hdfs://xxx"), new SimpleStringEncoder[String]("utf-8"))
      // 滚动策略
      .withRollingPolicy(
        DefaultRollingPolicy.builder
          .withRolloverInterval(1000)
          .withMaxPartSize(5 * 1024 * 1024)
          .build
      )
      // 分桶策略
      .withBucketAssigner(
        new DateTimeBucketAssigner[String]()
      )
      .withBucketCheckInterval(5)
      .withOutputFileConfig(
        OutputFileConfig
          .builder
          .withPartPrefix("youdi")
          .withPartSuffix(".txt")
          .build
      )
      .build


    // //.addSink()  /* SinkFunction实现类对象,用addSink() 来添加*/
    //
    //    ds.addSink(fileSink)
    ds.sinkTo(fileSink) /*Sink 的实现类对象,用 sinkTo()来添加  */


    val schema: Schema = Schema.createRecord("id", "user id", "com.youdi.User", true)
    val writeFactory0: ParquetWriterFactory[GenericRecord] = ParquetAvroWriters.forGenericRecord(schema)

    val schema1: Schema = SchemaBuilder
      .builder
      .record("com.youdi.avro.schema")
      .fields
      .requiredLong("uid")
      .name("info")
      .`type`()
      .map
      .values
      .`type`("string")
      .noDefault
      .endRecord


    val record: GenericData.Record = new GenericData.Record(schema)
    new GenericRecordAvroTypeInfo(schema)


    ParquetAvroWriters.forSpecificRecord(AvroEcen)



    env.execute(this.getClass.getName)
  }
}
