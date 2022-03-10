package com.hao.bigdata.flink.sinks
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-08:0:11
 * @Describe:
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      "hello",
      "world",
      "flink",
      "flinksql"
    )
    stream.addSink(
      new FlinkKafkaProducer[String](
        "leader:9092,slave1:9092,slave2:9092",
        "kafkasink",
        new SimpleStringSchema()
      )
    )
    env.execute()
  }
}
