package com.hao.bigdata.flink.alert
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:1:06
 * @Describe: 传感器温度值1内连续上升报警！！！
 */
object TempatureIncrAlert {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new SensorSource)
      .keyBy(_.id)
      .process(new MyAlert)
      .print()

    env.execute()
  }
  class MyAlert extends KeyedProcessFunction[String,SensorReading,String]{
    /**
     * 初始化一个状态变量，用来保存最近一次的温度值
     * 该状态变量定义为懒加载，惰性赋值
     * 只有当执行到process算子的时候才会初始化，所以是懒加载
     *  question： 为什么不用scala的变量？
     *      -状态变量可以通过设置，即通过检查点操作，保存在hdfs文件系统里
     *      -当程序故障时候，可以从最近的一次检查点回复
     *        因此需要一个名字和变量的类型（明确告诉flink状态变量的类型）
     * 状态变量只会初始化一次；如果没有状态变量，就初始化一个；如果有状态变量，即可直接读取；因此状态变量为单例模式
     */
    //初始化一个状态变量保存上次温度，懒加载
    lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last_temp", Types.of[Double])
    )
    // 初始化定时器的状态变量
    lazy val timerTs:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer_ts",Types.of[Long])
    )
    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]
                               ): Unit = {
      /**
       * 惰性变量：
       *    value方法是获取惰性变量的值
       *    update方法是更新惰性变量的值
       *    clear方法是清空惰性变量的状态
       */
      val previousTemp:Double = lastTemp.value()
      lastTemp.update(value.temperature)
      val curTimerTs:Long = timerTs.value()

      if (previousTemp == 0.0 || previousTemp > value.temperature){
        // 第一条数据 或者温度不上升
        // 移除当前的定时器
        ctx.timerService().deleteEventTimeTimer(curTimerTs)
        // 清空定时器的状态
        timerTs.clear()
      }else if (previousTemp < value.temperature && curTimerTs == 0L){
        // 温度上升了，但是还没有注册报警定时器。
        val timerTs_Init = ctx.timerService().currentProcessingTime() + 1 * 1000L
        // 注册定时器,当前机器时间 + 1 * 1000L
        ctx.timerService().registerProcessingTimeTimer(timerTs_Init)
        // 需要更新一下定时器！！！
        timerTs.update(timerTs_Init)
      }
    }
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]
                        ): Unit = {
      out.collect("传感器id为： " + ctx.getCurrentKey + " 的传感器温度值连续1s上升")
      // 清空定时器状态！！！否则会一直报警
      timerTs.clear()
    }
  }

}
