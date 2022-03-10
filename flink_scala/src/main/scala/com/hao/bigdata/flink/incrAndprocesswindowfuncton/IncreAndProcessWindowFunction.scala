package com.hao.bigdata.flink.incrAndprocesswindowfuncton
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-01:23:59
 * @Describe: 使用 增量聚合函数与全窗口增量聚合函数结合体 完成对窗口的平均数实现。
 *          为什么不单独使用增量聚合窗口或者全窗口聚合？
 *            - 使用两者的优点，同时规避掉缺点：
 *              -- 增量优势：仅仅保存累加器，性能消耗少，但是没有窗口信息！
 *              -- 全窗口优势：能够保存窗口信息；但是需要存储窗口全部元素的数据，消耗性能较大！！！
 */
object IncreAndProcessWindowFunction {
  // 封装全窗口聚合函数的返回值！
  case class RetrunData_IncrAndProcess(id:String,
                                       avgTemp:Double,
                                       windowStartTime:Long,
                                       windowEndTime:Long)
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment  = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    source.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new DIYAggregate_,new DIYProcessWindowFunction_)
      .print()
    env.execute()
  }
  class DIYAggregate_ extends AggregateFunction[SensorReading,(String,Double,Long),(String,Double)]{
    override def createAccumulator(): (String, Double, Long) = {
      ("",0.0,0L)
    }
    override def add(value: SensorReading, accumulator: (String, Double, Long)): (String, Double, Long) = {
      (value.id, value.temperature + accumulator._2, accumulator._3 + 1L)
    }
    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }
    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
  // 全窗口聚合函数的输入数据类型为增量聚合函数的输出数据类型，
  class DIYProcessWindowFunction_ extends ProcessWindowFunction[
    (String, Double),
    RetrunData_IncrAndProcess,
    String,
    TimeWindow]{
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Double)],
                          out: Collector[RetrunData_IncrAndProcess]): Unit = {
      val windowStartTime:Long = context.window.getStart
      val windowEndTime:Long = context.window.getEnd
      out.collect(new RetrunData_IncrAndProcess(key, elements.head._2, windowStartTime, windowEndTime))
    }
  }
}
