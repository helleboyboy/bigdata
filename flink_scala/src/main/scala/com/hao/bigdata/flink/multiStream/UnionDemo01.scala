package com.hao.bigdata.flink.multiStream

import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:0:06
 * @Describe: 前提：数据类型需要一致！！！
 *              顺序：FIFO方式进入队列进行合流
 *              注意：不会进行去重！！！
 *
 *              仅仅只是合并流而已！！！
 */
object UnionDemo01 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data1:DataStream[SensorReading] = env.addSource(new SensorSource).filter(data => data.id.equals("sensor_1"))
    val data2:DataStream[SensorReading] = env.addSource(new SensorSource).filter(data => data.id.equals("sensor_2"))
    val data3:DataStream[SensorReading] = env.addSource(new SensorSource).filter(data => data.id.equals("sensor_3"))
    val multiStreamUnion = data1.union(data2).union(data3)
    multiStreamUnion.print()
    env.execute()
  }
}

