package com.hao.bigdata.flink.source

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:18:08
 * @Describe: 数据源！！！！
 */
class SensorSource extends RichParallelSourceFunction[SensorReading]{

  var running: Boolean = true

  override def run(ctx: SourceContext[SensorReading]): Unit = {
    val rand = new Random

    //共有1到10序号的传感器！！！
    var curFTemp = (1 to 10).map(
      num => ("sensor_" + num, rand.nextGaussian() * 20)
    )

    while (running){
      //对十个传感器进行传值，传感器的温度会发生变化！！！
      curFTemp = curFTemp.map(oneData => (oneData._1,oneData._2 + rand.nextGaussian() * 0.5))
      // 获取当前时间
      val curTime = Calendar.getInstance().getTimeInMillis
      // 利用ctx上下文参数来发送数据到下游！！ 先collect再发
      curFTemp.foreach(idAndtemperature => ctx.collect(new SensorReading(idAndtemperature._1, curTime, idAndtemperature._2)))
      // 控制一下产生数据的频率！！
      TimeUnit.MILLISECONDS.sleep(300L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

