package com.hao.bigdata.flink.watermark
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-03:0:16
 * @Describe:
 */
object MyWaterMark {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L * 60)
    val source:DataStream[String] = env.socketTextStream("192.168.52.12", 8888)

    source.map(data => {
      val datas = data.split(" ")
      (datas(0),datas(1).toLong * 1000L)
    })
      .assignTimestampsAndWatermarks(new DIYWatermark)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult01)
      .print()
    env.execute("水位线的实现！！")
  }
  class DIYWatermark extends AssignerWithPeriodicWatermarks[(String,Long)]{
    // 设置最大延迟时间
    val delayTime:Long = 10 * 1000L
    // 系统观察到的元素包含的最大时间戳  +delayTime是为了防止下面的getCurrentWatermark方法结果范围溢出
    var maxEventTimeTs = Long.MinValue + delayTime
    // 产生水位线的逻辑 ， 默认每隔200毫秒调用一次
    override def getCurrentWatermark: Watermark = {
      // 最大的事件时间 - 最大延迟时间
      new Watermark(maxEventTimeTs - delayTime)
    }
    // 抽取时间戳！ 每来一条数据就调用一次该方法！！！！
    override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
      maxEventTimeTs = maxEventTimeTs.max(element._2) // 更新观察到的最大事件时间
      element._2 //将抽取的时间戳返回
    }
  }
  class WindowResult01 extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " 到 "
        + new Timestamp(context.window.getEnd) + " 的事件时间窗口元素存在 ： "
        + elements.size + "个！！！")
    }
  }
}
