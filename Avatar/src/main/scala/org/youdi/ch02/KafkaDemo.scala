package org.youdi.ch02

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.time.Duration
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import java.util.Properties


object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "local:9092")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("group.id", "g1")
    properties.setProperty("enable.auto.commit", "true")

    val source: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      "test",
      new SimpleStringSchema(),
      properties
    )

    val ds: DataStream[String] = env.addSource(source)
    ds.print()


    val source1: KafkaSource[String] = KafkaSource
      .builder()
      .setBootstrapServers("local:9092")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setTopics("flink-01")
      .setGroupId("fk03")
      .build()

    val strategy: WatermarkStrategy[String] = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ZERO)
      .withTimestampAssigner(
        new SerializableTimestampAssigner[String]() {
          override def extractTimestamp(element: String, recordTimestamp: Long) = {
            val arr: Array[String] = element.split(",")
            arr(3).toLong
          }
        }
      )


    val stream: DataStream[String] = env.fromSource(source1, strategy, " ").setParallelism(2)

    stream.print()

    env.execute("kafka source")

  }
}
