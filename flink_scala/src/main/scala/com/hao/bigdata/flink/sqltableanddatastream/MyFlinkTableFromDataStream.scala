package com.hao.bigdata.flink.sqltableanddatastream

import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-10:22:11
 * @Describe:
 */
object MyFlinkTableFromDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)
    val sourceDataStream:DataStream[SensorReading] = env.addSource(new SensorSource)
    val table:Table = tEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature)
    table.select('id, 'ts, 'temperature)

    tEnv.toAppendStream[Row](table).print()
    env.execute()
  }
}
