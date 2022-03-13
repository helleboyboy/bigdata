package com.hao.bigdata.flink.tableAndsql.groupWindows.table

import com.hao.bigdata.flink.source.SensorSource
import org.apache.calcite.rex.RexWindowBounds.preceding
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:18:34
 * @Describe:
 */
object OverWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream = env.addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val table = tEnv.fromDataStream(sourceStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    table
      .window(Over partitionBy 'id orderBy 'ts preceding UNBOUNDED_RANGE as 'w)
      //不需要再groupby，select的时候，需要加上over 窗口参数！
      .select('id, 'id.count over 'w)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}
