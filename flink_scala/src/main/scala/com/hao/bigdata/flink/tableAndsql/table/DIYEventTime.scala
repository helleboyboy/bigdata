package com.hao.bigdata.flink.tableAndsql.table

import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:15:19
 * @Describe:
 */
object DIYEventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.addSource(new SensorSource)
      //定义水位线提取的时间戳字段和最大延迟时间
      .assignAscendingTimestamps(_.timestamp)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)
    // 事件时间的时间戳可以由 原有的字段调用.rowtime来指定！！！！
    val table = tEnv.fromDataStream(stream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    table
      //使用Tumble类来定义滚动窗口
      .window(Tumble over 10.seconds on 'ts as 'w)
      //根据id和窗口信息来分组
      .groupBy('id,'w)
      .select('id, 'id.count)
      // 需要定义输出的数据类型！
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}
