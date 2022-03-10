package com.hao.bigdata.flink.lateddata
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:18:00
 * @Describe:
 */
object DIYProcessLateData {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("192.168.52.12", 8888)
      .map(data => {
        val datas = data.split(" ")
        (datas(0),datas(1).toLong * 1000L)
      })
      // 数据升序！
      .assignAscendingTimestamps(_._2)
      // 没有开窗，即时间保持升序即不会有迟到数据！！！
      .process(new DIYProcessLateDataAction)
    stream.print()
    stream.getSideOutput(new OutputTag[String]("lateData")).print()
    env.execute()
  }
  class  DIYProcessLateDataAction extends ProcessFunction[(String,Long),(String,Long)] {
    val late = new OutputTag[String]("lateData")

    override def processElement(value: (String, Long),
                                ctx: ProcessFunction[(String, Long),
                                  (String, Long)]#Context,
                                out: Collector[(String, Long)]
                               ): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        // 第一个参数为输出标签名
        ctx.output(late, "迟到事件来了,value ： " + value)
      } else {
        // 数据没有迟到就直接发送！
        out.collect(value)
      }
    }
  }
}
