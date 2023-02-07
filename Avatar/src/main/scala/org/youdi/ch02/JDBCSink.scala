package org.youdi.ch02

import com.mysql.cj.jdbc.MysqlXADataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.function.SerializableSupplier

import java.sql.PreparedStatement
import javax.sql.XADataSource

object JDBCSink {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()
    configuration.setInteger("rest.port", 8822)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("hdfs://xxxx")

    val ds: DataStream[String] = env.socketTextStream("localhost", 9998)

    val sink: SinkFunction[String] = JdbcSink.sink(
      "insert into t_eventlog values (?) on duplicate key update id=?",
      new JdbcStatementBuilder[String]() {
        // 有异常抛出
        override def accept(t: PreparedStatement, u: String) = {
          t.setString(1, u)
        }
      },
      JdbcExecutionOptions.builder
        .withMaxRetries(3)
        .withBatchSize(1)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://doit01:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
        .withUsername("root")
        .withPassword("1111")
        .build()
    )

    ds.addSink(sink)


    val sinkFunction: SinkFunction[String] = JdbcSink.exactlyOnceSink(
      "insert into t_eventlog values (?)",
      new JdbcStatementBuilder[String]() {
        // 有异常抛出
        override def accept(t: PreparedStatement, u: String) = {
          t.setString(1, u)
        }
      },
      JdbcExecutionOptions.builder
        .withMaxRetries(3)
        .withBatchSize(1)
        .build(),
      JdbcExactlyOnceOptions.builder()
        // // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
        .withTransactionPerConnection(true)
        .build(),
      new SerializableSupplier[XADataSource]() {
        override def get(): XADataSource = {
          val source: MysqlXADataSource = new MysqlXADataSource
          source.setURL("jdbc:mysql://doit01:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
          source.setUser("root")
          source.setPassword("123")
          source
        }
      }
    )
    ds.addSink(sinkFunction)
    env.execute(this.getClass.getName)
  }
}
