package org.youdi.ch02

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object KafkaSinkOperator {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()
    configuration.setInteger("rest.port", 8822)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)

    val sink: KafkaSink[String] = KafkaSink
      .builder()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder()
          .setTopic("test")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setTransactionalIdPrefix("youdi")
      .build()

    ds.sinkTo(sink)
    env.execute(this.getClass.getName)
  }

}
