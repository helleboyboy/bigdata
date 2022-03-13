package com.hao.bigdata.flink.tableAndsql.groupWindows.table
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:18:09
 * @Describe:
 *    完成over windows的实现
 *    多去了解hive的开窗函数！
 */
object OverWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceDataStream = env.addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.createTemporaryView("sensor_overWindows", sourceDataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    val sqlString = "SELECT id, COUNT(id) OVER(PARTITION BY id ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM sensor_overWindows"
    tEnv.sqlQuery(sqlString)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}