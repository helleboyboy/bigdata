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
 * @Date: 2022-03-03:22:47
 * @Describe: 不同流的水位线传播规则！！！先比较水位线发送到下游，后更新水位线！！！
 */
object WatermarkRule {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1: DataStream[(String, Long)] = env.socketTextStream("192.168.52.12", 8888)
      .map(data => {
        val datas = data.split(" ")
        (datas(0), datas(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)


    val stream2: DataStream[(String, Long)] = env.socketTextStream("192.168.52.12", 8889)
      .map(data => {
        val datas = data.split(" ")
        (datas(0), datas(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)

    stream1.union(stream2)
      .keyBy(_._1)
      .process(new DIYKeyedProcessFunction)
      .print()
    env.execute("查看水位线传播规则！！！")
  }
  class DIYKeyedProcessFunction extends KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {
      out.collect("当前的水位线是： ---" + ctx.timerService().currentWatermark())
    }
  }
}
