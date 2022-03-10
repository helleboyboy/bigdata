package com.hao.bigdata.flink.increwindowfunction
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-01:23:23
 * @Describe: 使用增量聚合函数来实现窗口的平均数计算！！
 */
object AvgTempAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 下面开窗必须设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    source.keyBy(_.id) // 先分流
      .timeWindow(Time.seconds(5)) // 再开窗
      .aggregate(new DIYAggregate) // 聚合函数操作
      .print()
    env.execute()
  }
  /**
   *    Type parameters:
   *    <IN> – The type of the values that are aggregated (input values)
   *    <ACC> – The type of the accumulator (intermediate aggregate state).
   *    <OUT> – The type of the aggregated result
   *    参数解析：
   *      1.第一个参数：输入类型：sensorReading
   *      2.第二个参数：累加器类型： [传感器id，温度和累加，传来的数据条数]
   *      3.第三个参数：返回值类型： [传感器id，平均温度值]
   */
  class DIYAggregate extends AggregateFunction[SensorReading,(String,Double,Long),(String,Double)]{
    // 先初始化累加器，定义累加器的数据类型
    override def createAccumulator(): (String, Double, Long) = {
      ("",0.0,0L) // 原始累加器
    }
    // 每来一条数据都会调用该方法，即累加操作如下：
    override def add(value: SensorReading, accumulator: (String, Double, Long)): (String, Double, Long) = {
      (value.id, accumulator._2 + value.temperature, accumulator._3 + 1L)
    }
    // 当窗口关闭的时候，调用此方法，发送结果到下游
    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }
    // merge方法为事件窗口使用
    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}
