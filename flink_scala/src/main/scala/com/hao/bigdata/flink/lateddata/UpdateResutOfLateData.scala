package com.hao.bigdata.flink.lateddata

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
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
 * @Date: 2022-03-05:20:21
 * @Describe:
 */
object UpdateResutOfLateData {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("192.168.31.33", 8888)
      .map(data =>{
        val datas = data.split(" ")
        (datas(0),datas(1).toLong * 1000L)
      })
//      设置事件时间的最大延迟时间
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 提取事件时间
        override def extractTimestamp(element: (String, Long)): Long = {
          element._2
        }
      })
      .keyBy(_._1)
//      设置窗口大小
      .timeWindow(Time.seconds(5))
//      设置允许延迟时间
      .allowedLateness(Time.seconds(5))
      .process(new UpdateResultOfLateDataAction)
    stream.print()
    env.execute()
  }
  class UpdateResultOfLateDataAction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]
                        ): Unit = {
      // 需要获取到窗口状态变量,默认值为false；窗口信息只有水位线 超过窗口结束时间 才会有变化！！！
      val windowStateVar = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("update_windowResult", Types.of[Boolean])
      )
      if (!windowStateVar.value()){ // 即当水位线没过窗口结束的时间。
        out.collect("窗口触碰到了水位线了，元素数量为： " + elements.size)
        // 更新窗口变量的值
        windowStateVar.update(true)
      }else{
        out.collect("这是迟到元素数据，更新后的元素个数为 : "+ elements.size)
      }
    }
  }
}