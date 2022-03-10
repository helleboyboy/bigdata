package com.hao.bigdata.flink.sqltableanddatastream

import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-10:23:58
 * @Describe:
 */
object FlinkTableRetract {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDataStream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.createTemporaryView("COUNT_ID",sourceDataStream)
    val query = "select id, count(id) from COUNT_ID group by id"
    val table = tEnv.sqlQuery(query)
    tEnv.toRetractStream[Row](table).print()
    env.execute()
  }
}