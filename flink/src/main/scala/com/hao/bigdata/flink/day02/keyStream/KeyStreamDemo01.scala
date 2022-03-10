package com.hao.bigdata.flink.day02.keyStream

import com.hao.bigdata.flink.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:23:22
 * @Describe:
 */
object KeyStreamDemo01 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val source:DataStream[SensorReading] = env.addSource(new SensorSource).filter(data => data.id.equals("sensor_1"))
    /**
     * 泛型解析：
     *  1.第一个泛型为元素类型
     *  2.第二个泛型为key的数据类型
     */
    val keyedStreamData:KeyedStream[SensorReading,String] = source.keyBy(data => data.id)
    /*
    注意点：
      下面的max操作和reduce操作都是同一组key里面进行比较的！！！
      max与reduce的比较：
        1、max得到的结果：一个传来的sensorReading数据，即不会改变原来的值
        2.reduce得到的结果，第二项和第二项的元素项都是固定的，但是温度值是拿max的！！！
      keyedStream API可以将数据类型由DataStream转换为keyedStream
      滚动聚合操作后，数据类型又由keyedStream转换为DataStream
     */
    //使用max来拿到同一组key的最高温度值
//    keyedStreamData.max(2).print()

    // reduce算子是两两比较后，得到结果再以后一个元素进行比较
    keyedStreamData.reduce(
      (data1,data2) => new SensorReading(data1.id , data2.timestamp , data1.temperature.max(data2.temperature))
    ).print()
    env.execute()
  }
}
