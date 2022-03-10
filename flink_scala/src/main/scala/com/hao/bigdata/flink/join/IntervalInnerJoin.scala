package com.hao.bigdata.flink.join
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-06:13:56
 * @Describe: 目前只支持事件时间和inner join；多条流麻烦！！！
 */
object IntervalInnerJoin {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val clickStream:KeyedStream[(String,String,Long),String] = env.fromElements(
      ("1","click", 72 * 1000L),
      ("2","click", 36 * 1000L)
    )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    val lookupStream:KeyedStream[(String,String,Long),String] = env.fromElements(
      ("1","lookup",70 * 1000L),
      ("1","lookup",73 * 1000L),
      ("1","lookup",10 * 1000L),
      ("1","lookup",90 * 1000L),
      ("2","lookup",34 * 1000L),
      ("2","lookup",35 * 1000L),
      ("2","lookup",37 * 1000L)
    )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    clickStream
      .intervalJoin(lookupStream)
      // 两条流的位置可以改变，但是对应的between的参数值要随之改变！！！
      .between(Time.seconds(-1),Time.seconds(1))
      .process(new DIYInnerJoinAction)
      .print()
    env.execute()
  }
  class DIYInnerJoinAction extends ProcessJoinFunction[(String,String,Long),(String,String,Long),String]{
    override def processElement(
                                 left: (String, String, Long),
                                 right: (String, String, Long),
                                 ctx: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context,
                                 out: Collector[String]): Unit = {
      out.collect(left + "===>" + right)
    }
  }
}