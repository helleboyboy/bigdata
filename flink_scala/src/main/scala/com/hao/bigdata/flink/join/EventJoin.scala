package com.hao.bigdata.flink.join
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-06:14:24
 * @Describe:   基于事件时间的join：
 *                  1.首先将数据分流
 *                  2.将数据根据时间戳分为不同的窗口
 *                  3.对同一个窗口的数据进行笛卡尔积！！！！
 *                  4.可以在JoinFunction类中进行筛选
 */
object EventJoin {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream1:DataStream[(String,Int,Long)] = env.fromElements(
      ("a",1,1000L * 2),

      ("b",1,1000L * 2),
      ("b",2,1000L * 5)
    )
      .assignAscendingTimestamps(_._3)

    val stream2:DataStream[(String,Int,Long)] = env.fromElements(
      ("a",10,1000L * 1),
      ("a",20,1000L * 2),
      ("a",30,1000L * 3),

      ("b",53,1000L * 3),  //会根据第三个字段判断属于哪个窗口的！！！
      ("b",54,1000L * 4),
      ("b",56,1000L * 6),
      ("b",57,1000L * 7),
      ("b",10,1000L * 8) // 并没有数据，因为不在 4 ~ 4+4 区间内,左闭右开区间！
    )
      .assignAscendingTimestamps(_._3)

    stream1.join(stream2)
      .where(_._1) // 第一条流的key
      .equalTo(_._1) // 第二条流的key
      .window(TumblingEventTimeWindows.of(Time.seconds(4))) // 如果不知道参数的类型，可以点进去方法查看，ctrl+H
      .apply(new DIYEventJoinFunctionAction)
      .print()
    env.execute()
  }
  class DIYEventJoinFunctionAction extends JoinFunction[(String,Int,Long),(String,Int,Long),String]{
    override def join(first: (String, Int, Long),
                      second: (String, Int, Long)
                     ): String = {
      // 其实这里可以做筛选条件！！！！
      first + " ===> " + second
    }
  }
}
