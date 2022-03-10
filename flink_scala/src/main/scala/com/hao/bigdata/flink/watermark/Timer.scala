package com.hao.bigdata.flink.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-03:23:52
 * @Describe:
 */
object Timer {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("192.168.52.12",8888)
      .map(data => {
        val datas = data.split(" ")
        (datas(0),datas(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new KeyBy_ProcessTime)
//      .process(new KeyBy)
      .print()

    env.execute("定时器，触发定时器的原则时：水位线大于定时器的时间戳")
  }
  class KeyBy extends KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {
      // 给元素的时间戳注册一个定时器， 时间戳+ 10s  事件时间
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, (String, Long),
                           String]#OnTimerContext, out: Collector[String]
                        ): Unit = {

      out.collect("定时器触发了！！！定时器执行的时间戳为： "  + timestamp )
    }
  }

  class KeyBy_ProcessTime extends KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //      机器时间: 当前机器时间 + 10s
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("定时器被出发了，定时器执行的时间戳为 ： "  + timestamp)
    }
  }

}
