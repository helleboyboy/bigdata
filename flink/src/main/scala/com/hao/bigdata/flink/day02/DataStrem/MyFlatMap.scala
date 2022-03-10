package com.hao.bigdata.flink.day02.DataStrem

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:21:41
 * @Describe: flatmap针对流中的每一个元素，生成0个或者不定个结果！！！
 */
object MyFlatMap {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source:DataStream[String] = env.fromElements("white", "gray", "black")
    source.flatMap(new MyDIYFlatMap)
      .print()
    env.execute()
  }
  /**
   * 自定义flatMap
   *  1.继承类
   *  2.补充类泛型
   *  3.重写方法（写逻辑） #flink常常用集合的collect方法收集元素从而下发到下游
   */
  class MyDIYFlatMap extends FlatMapFunction[String,String]{
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value.equals("white")){
        out.collect("white")
      }else if(value.equals("gray")){
        out.collect("gray")
        out.collect("gray")
      }
    }
  }

}
