package com.hao.bigdata.flink.watermark
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-02:21:33
 * @Describe:
 */
object WaterMarkDemo {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 需要设置并行度为1，否则得不到测试的结果效果
    env.setParallelism(1)
//    设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    设置数据源，hostname要细心
    val source:DataStream[String] = env.socketTextStream("192.168.52.12", 8888)
//    函数规范： data => {} 而不是data => ()
    val result = source.map(data => {
      val datas = data.split(" ")
      // 数组下标用小括号（）
      (datas(0), datas(1).toLong * 1000L)
    })
//      设置水位线
      .assignTimestampsAndWatermarks(
//        设置最大延迟时间为5s，泛型为流元素的类型！！！
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 提取时间，在数据里面提取！！！
          override def extractTimestamp(element: (String, Long)): Long = {
            element._2
          }
        }
      )
      // 分流，一定要在分流之前设置水位线，不然数据可能跨机器传输，会有较大的影响。
      .keyBy(_._1)
      // 开了 10s大小的窗口
      .timeWindow(Time.seconds(10))
      // 聚合操作！！全窗口函数
      .process(new WindowResult)

    result.print()
    env.execute()
  }
  class WindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Long)],
                          out: Collector[String]
                        ): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " 到 " + new Timestamp(context.window.getEnd) + " 的事件时间窗口元素存在 ： " + elements.size + "个！！！")
    }
  }

}
