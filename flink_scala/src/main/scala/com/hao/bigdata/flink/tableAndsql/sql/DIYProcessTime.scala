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
 * @Date: 2022-03-13:16:10
 * @Describe:
 *    1.使用sql风格，需要用到createTemporayView来定义表
 *    2.表，字段的大小写尽量保持一致
 *    3.利用TUMBLE 、 HOP来定义滚动窗口和滑动窗口。其中没有复数！！！
 *    4.处理时间需要自主添加！
 */
object DIYProcessTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.addSource(new SensorSource)
      //定义水位线所需要提取的时间戳，以及最大延迟时间
      .assignAscendingTimestamps(_.timestamp)
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    //如果用到sql类型的，需要调用.createTemporaryView，表名需要一一对应
    // 处理时间需要在所有字段后面自主添加！ 调用.proctime方法来确定处理时间字段！！
    tEnv.createTemporaryView("sensor",sourceDataStream,'id, 'timestamp as 'ts, 'temperature, 'pc.proctime)
//     表名区分大小写！！ TUMBLE来定义一个滚动窗口，第一个参数为处理时间字段，第二个参数为滚动窗口长度
//    val sqlString = "SELECT id, COUNT(id) FROM sensor GROUP BY id, TUMBLE(pc, INTERVAL '10' second)"
//    表名区分大小写！！ HOP 来定义一个滑动窗口，第一个参数为处理时间字段，第二个参数为滑动长度，第三个参数为窗口长度
    val sqlString = "SELECT id, COUNT(id) FROM sensor GROUP BY id, HOP(pc, INTERVAL '5' second, INTERVAL '10' second)"
    tEnv.sqlQuery(sqlString)
      // 输出类型定义为Row
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}
