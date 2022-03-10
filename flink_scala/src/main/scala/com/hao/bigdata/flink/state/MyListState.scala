package com.hao.bigdata.flink.state
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.{FsStateBackend, FsStateBackendFactory}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-06:16:49
 * @Describe: 使用列表状态！！！
 */
object MyListState {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 设定9 * 1000L毫秒进行checkpoints
    env.enableCheckpointing(1000L)
    // 设定checkpoints保存的目录文件
    env.setStateBackend(
      new FsStateBackend(
        "file:\\D:\\Data-All\\idea_data\\bigdata\\flink_scala\\src\\main\\resources\\checkpoints"))

    val stram = env.addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyed)
      .print()
    env.execute()
  }
  class MyKeyed extends KeyedProcessFunction[String,SensorReading,String]{
    lazy val listStateVal = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list_state",Types.of[SensorReading])
    )

    lazy val valueStateVal = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("value_state",Types.of[Long])
    )

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]
                               ): Unit = {
      listStateVal.add(value)
      if (valueStateVal.value() == 0L){
        val ts:Long = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        valueStateVal.update(ts) // 状态变量必须要更新才会生效！！！
      }
    }
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]
                        ): Unit = {
      // 状态变量无法直接直接计数！！！
      val readings:ListBuffer[SensorReading] = new ListBuffer[SensorReading]
      //导入隐式转换依赖
      import scala.collection.JavaConversions._
      for (i <- listStateVal.get()){
        readings += i
      }
      out.collect("当前时刻" + timestamp + "列表状态变量里面共有 " + readings.size + "个！")
      valueStateVal.clear() //清空值状态的值
    }
  }
}
