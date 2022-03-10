package com.hao.bigdata.flink.sqltableanddatastream

import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-10:22:30
 * @Describe:
 */
object MyFlinkViewFromDataStreamOrTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceDataStream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val table:Table = tEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature)

    table.select('id, 'ts, 'temperature)
//    tEnv.toAppendStream[Row](table).print()   //追加流
//    tEnv.toRetractStream[Row](table).print()   //更新模式， 第一个数据类型为boolean，若为true即为追加，若为false则为更新
    //reture datas:(true,+I[sensor_10, 1646925098512, 14.833193685139449])
    // 建议 datastream -> table -> datastream
    val sinkDataStream:DataStream[SensorReading] = tEnv.toAppendStream[SensorReading](table)
    sinkDataStream.print()

//    视图
//    tEnv.createTemporaryView("sensor_view",table)
//    val statement = "select id, temperature from sensor_view"
//    tEnv.executeSql(statement).print()
    env.execute()

  }
}
