package com.hao.bigdata.flink.tableAndsql.sql
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:16:31
 * @Describe:
 *    1.实现sql风格的事件时间定义
 *    2.利用.rowtime来取时间戳字段！
 */
object DIYEventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream = env.addSource(new SensorSource)
      //设置水位西安提取的时间戳和最大延迟时间
      .assignAscendingTimestamps(_.timestamp)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.createTemporaryView("sensor_eventTime", sourceStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    val sqlString = "SELECT id, COUNT(id) FROM sensor_eventTime GROUP BY id, TUMBLE(ts, INTERVAL '10' second)"
    tEnv.sqlQuery(sqlString)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}