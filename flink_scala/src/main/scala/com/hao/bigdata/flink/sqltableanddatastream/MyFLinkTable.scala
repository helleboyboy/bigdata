package com.hao.bigdata.flink.sqltableanddatastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row
import org.apache.flink.table.api._
import org.apache.flink.api._
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-10:0:08
 * @Describe:
 */
object MyFLinkTable {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val setting = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, setting)

    tEnv
      .connect(new FileSystem().path("D:\\Data-All\\idea_data\\bigdata\\flink_scala\\src\\main\\resources\\data.csv"))
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id",DataTypes.STRING()) //数据类型要转换一下！！！
          .field("ts",DataTypes.STRING())
          .field("temperature",DataTypes.STRING())
      )
      .createTemporaryTable("myTable") //创建临时表

    val myTable:Table = tEnv.from("myTable")
    val result = myTable.select("id , ts")
    tEnv.toAppendStream[Row](result).print()
    env.execute()
  }
}