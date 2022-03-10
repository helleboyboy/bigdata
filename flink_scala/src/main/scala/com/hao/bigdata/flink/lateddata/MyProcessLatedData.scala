package com.hao.bigdata.flink.lateddata
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:17:40
 * @Describe:
 *        设置迟到数据发送到侧输出流
 *        sideOutputLateData方法为将迟到数据发送到侧输出
 *        stream.getSideOutput(new OutputTag[(String,Long)]("late_data")).print() 将迟到数据打印
 */
object MyProcessLatedData {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val stream = env.socketTextStream("192.168.52.12", 8888)
    val stream = env.socketTextStream("192.168.31.33", 8888)
      .map(data => {
        val datas = data.split(" ")
        (datas(0),datas(1).toLong * 1000L)
      })
      // 设置为升序的数据！！！即最大延迟时间为 0s
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      //将迟到的数据发送到侧输出流！！！
      .sideOutputLateData(new OutputTag[(String, Long)]("late_data"))
      .process(new MyProcessLatedDataAction)
    stream.print()
    // 打印侧输出流的迟到数据
    stream.getSideOutput(new OutputTag[(String,Long)]("late_data")).print()
    env.execute()
  }
  class MyProcessLatedDataAction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]
                        ): Unit = {
      out.collect("窗口中有 ： " + elements.size + "个元素")
    }
  }
}
