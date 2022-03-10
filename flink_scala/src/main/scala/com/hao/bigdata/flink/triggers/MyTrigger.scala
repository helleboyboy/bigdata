package com.hao.bigdata.flink.triggers
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:22:42
 * @Describe: 注意点：
 *                  1.系统会每隔200ms插入一条水位线
 */
object MyTrigger {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("192.168.31.33", 8888)
      .map(data => {
        val datas = data.split(" ")
        (datas(0),datas(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .trigger(new MyTriggerAction)
      .process(new WindowCount)
    stream.print()
    env.execute()
  }
  class MyTriggerAction extends Trigger[(String,Long),TimeWindow]{
    // 每条数据进来都会调用该方法！在整数秒和窗口关闭的时候触发窗口计算
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext
                          ): TriggerResult = {
      val isFirstVar = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("isFirst", Types.of[Boolean]))
      if (!isFirstVar.value()){
        // ts和上一个水位线会更好相差1000ms = 1s
        val ts = ctx.getCurrentWatermark + (1000 - ctx.getCurrentWatermark % 1000)
        ctx.registerEventTimeTimer(ts) //第一条数据的整数秒注册一个定时器
        ctx.registerEventTimeTimer(window.getEnd) //在窗口关闭时间时注册一个定时器
        isFirstVar.update(true)
      }
      TriggerResult.CONTINUE
    }
    // 使用的是事件时间，所以直接continue
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    /**
     * @KNOW   Called when an event-time timer that was set using the trigger context fires.
     *         所以这里指的是onElement方法中注册的事件定时器被触发后即会调用onEventTime.这里就是第二秒！
     * @param time
     * @param window
     * @param ctx
     * @return
     */
    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext
                            ): TriggerResult = {
//      onElement方法注册过的窗口结束时间的定时器
      if (time == window.getEnd){
        // 在窗口闭合时候，触发计算并清空状态
        TriggerResult.FIRE_AND_PURGE
      }else{
        val t = ctx.getCurrentWatermark + (1000 - ctx.getCurrentWatermark % 1000)
        // 需要在窗口关闭前注册定时器才有意义！！！关闭后就无意义了
        if (t < window.getEnd){
          ctx.registerEventTimeTimer(t) // 注册整数秒的定时器
        }
        TriggerResult.FIRE // 触发计算
      }
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      val isFirstVar = ctx.getPartitionedState(new ValueStateDescriptor[Boolean](
        "isFirst", Types.of[Boolean]
      ))
      isFirstVar.clear()
    }
  }
  class WindowCount extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]
                        ): Unit = {
      out.collect("窗口有 " + elements.size + " 条数据，关闭窗口时间为： " + context.window.getEnd)
    }
  }
}
