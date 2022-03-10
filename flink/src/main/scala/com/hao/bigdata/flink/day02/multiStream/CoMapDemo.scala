package com.hao.bigdata.flink.day02.multiStream

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:0:20
 * @Describe:
 */
object CoMapDemo {
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
    afterConnection.map(new MyDIYCoMap).print()
    env.execute()
  }
  /**
   * 对两条流进行处理的comap方法！！！
   */
  class MyDIYCoMap extends CoMapFunction[(String,Long),(String,Long),String]{
    //对第一条流进行处理
    override def map1(value1: (String, Long)): String = {
      "age: " + value1._2 + " , 工资为： " + value1._1
    }
    // 对第二条流进行处理
    override def map2(value2: (String, Long)): String = {
      "age: " + value2._2 + " , 工资为： " + value2._1
    }
  }
}
