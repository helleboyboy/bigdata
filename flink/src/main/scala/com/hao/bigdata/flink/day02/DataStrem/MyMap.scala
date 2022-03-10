package com.hao.bigdata.flink.day02.DataStrem

import com.hao.bigdata.flink.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:21:03
 * @Describe:
 */
object MyMap {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source : DataStream[SensorReading] = env.addSource(new SensorSource)
    val transformation = source.map(new MyDIYMap)
//    val transformation : DataStream[String] = source.map(data => data.id)
    transformation.print()
    env.execute()
  }

  /**
   * 自定义map函数！
   *  1.了解到需要继承哪个类
   *  2.补全类的泛型！（源码有！！）
   *  3.重写方法：逻辑体现
   */
  class MyDIYMap extends MapFunction[SensorReading,String]{
    override def map(value: SensorReading): String = {
      return value.id
    }
  }
}
