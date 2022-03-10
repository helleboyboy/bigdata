package com.hao.bigdata.flink.processwindowfunction
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
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
 * @Date: 2022-03-01:22:36
 * @Describe: 窗口内的平均温度实现
 */
object AvgTempProcessWindowFunction {
  case class RetrunData(id:String,
                        avgTemp:Double,
                        windowStartTime:Long,
                        windowEndTime:Long)
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    source.keyBy(_.id) //分流
      .timeWindow(Time.seconds(5)) // 开窗
      .process(new DIYProcessWindowFuntion) //聚合
      .print()
    env.execute()
  }
  /**
   *  @tparam IN The type of the input value.
      @tparam OUT The type of the output value.
      @tparam KEY The type of the key.
      @tparam W The type of the window.
   */
  class DIYProcessWindowFuntion extends ProcessWindowFunction[SensorReading,RetrunData,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[RetrunData]): Unit = {
      var count = elements.size
      var sum = 0.0
      for (elem <- elements) {
        sum += elem.temperature
      }
      // 单位为毫秒
      val windowStartTime:Long = context.window.getStart
      val windowEndTime:Long = context.window.getEnd
      out.collect(new RetrunData(key , sum / count , windowStartTime , windowEndTime))
    }
  }
}
