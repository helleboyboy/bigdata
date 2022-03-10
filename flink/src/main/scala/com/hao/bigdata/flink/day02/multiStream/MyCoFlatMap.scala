package com.hao.bigdata.flink.day02.multiStream

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:0:35
 * @Describe:
 *    那条流进行FIFO中，就会进入那条流的coflatmap方法中
 */
object MyCoFlatMap {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val one:DataStream[(String,Long)] = env.fromElements(
      ("first", 8000L),
      ("second", 16000L)
    )
    val two:DataStream[(String,Long)] = env.fromElements(
      ("first",24L),
      ("second",25L)
    )
    //直接合并两条流是没什么意义的！！！！要由关联才行！！！
    val afterConnection:ConnectedStreams[(String,Long),(String,Long)] = one.keyBy(0).connect(two.keyBy(0))
    afterConnection.flatMap(new MyDIYCoFlatMap).print()
    env.execute()
  }
  class MyDIYCoFlatMap extends CoFlatMapFunction[(String,Long),(String,Long),String]{
    override def flatMap1(value1: (String, Long), collector: Collector[String]): Unit = {
      collector.collect("age: " + value1._2 + " , 工资为： " + value1._1)
      collector.collect("age: " + value1._2 + " , 工资为： " + value1._1)
    }
    override def flatMap2(value2: (String, Long), collector: Collector[String]): Unit = {
      collector.collect("age: " + value2._2 + " , 工资为： " + value2._1)
    }
  }
}
