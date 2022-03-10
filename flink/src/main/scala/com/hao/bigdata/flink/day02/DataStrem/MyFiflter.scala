package com.hao.bigdata.flink.day02.DataStrem

import com.hao.bigdata.flink.day02.{SensorReading, SensorSource}
import com.hao.bigdata.flink.day02.SensorReading
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:21:20
 * @Describe:
 */
object MyFiflter {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    source.map(data => data.id)
//      .filter(id => !id.equals("sensor_1"))
      .filter(new MyDIYFiflter)
      .print()
    env.execute()
  }
  /**
   * 自定义Fiflter方法
   *  1.继承FiflterFunction类
   *  2.补充泛型类型
   *  3.重写方法，即逻辑！！
   */
  class MyDIYFiflter extends FilterFunction[String]{
    override def filter(value: String): Boolean = {
      return value.equals("sensor_1")
    }
  }
}
