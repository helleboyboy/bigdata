package com.hao.bigdata.flink.sinks
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-08:0:24
 * @Describe:
 */
object GetKafkaOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.put("bootstrap.servers","leader:9092,slave1:9092,slave2:9092")
    prop.put("group.id","consumer-group")
    prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("auto.offset.reset","latest")
    val stram = env.addSource(
      new FlinkKafkaConsumer[String](
        "kafkasink",
        new SimpleStringSchema(),
        prop
      )
    )
    stram.print()
    env.execute()
  }
}