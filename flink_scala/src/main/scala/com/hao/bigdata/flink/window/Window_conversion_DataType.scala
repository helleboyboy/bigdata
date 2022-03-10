package com.hao.bigdata.flink.window
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:23:59
 * @Describe: 数据类型的转换：
 *                datastream[SensorReading] -> keyedStream[SensorReading,String] -> windowedStream[SensorReading,String,TimeWindow] -> DataStream[SensorReading]
 */
object Window_conversion_DataType {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 开窗操作需要用env设置好时间语义！！
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    val keyedStreamTransformation:KeyedStream[SensorReading,String] = source.keyBy(data => data.id)
    val windowTransformation:WindowedStream[SensorReading,String,TimeWindow] = {
      keyedStreamTransformation.timeWindow(Time.seconds(5),Time.seconds(5))
    }
    // 聚合操作的数据类型会转换为DataStream类型
    val result:DataStream[SensorReading] = windowTransformation
      .reduce((data1, data2) => new SensorReading(data1.id, 0L, data1.temperature.min(data2.temperature)))
    result.print()
    env.execute()
  }
}
