package com.hao.bigdata.flink.sqltableanddatastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.api._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-10:21:19
 * @Describe:
 */
object MyFlinkSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.connect(new FileSystem().path("D:\\Data-All\\idea_data\\bigdata\\flink_scala\\src\\main\\resources\\data.csv"))
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id",DataTypes.STRING())
          .field("ts",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      ).createTemporaryTable("MyFlinkSql")

    val table:Table = tEnv.from("MyFlinkSql")
      .select('id, 'ts, 'temperature)
      .filter("id = 'sensor_1'")

    // 使用的是tEnv！！！
    tEnv.toAppendStream[Row](table).print()

    val sql = "select id, ts, temperature from MyFlinkSql where id = 'sensor_2'"
    val table2 = tEnv.sqlQuery(sql)
    tEnv.toAppendStream[Row](table2).print()

    env.execute()

  }
}
