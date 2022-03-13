package com.hao.bigdata.flink.tableAndsql.groupWindows.table
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:14:29
 * @Describe:
 *    Table定会使用处理时间
 *    窗口分别使用了滚动窗口和滑动窗口
 *    注意项：
 *        1.处理时间需要新添加字段 如 'pc.proctime
 *        2.开窗函数使用window，里面参数分别用Tumble类和Slide类来定义滚动和滑动窗口
 *        3.窗口里面参数需要起别名
 *        4.先window后groupby
 */
object DIYProcessTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.addSource(new SensorSource)
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
//    处理时间需要在最后一个字段定义，而且要用到.proctime方法
    val table = tEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature, 'pc.proctime)
    table
//      需要先定义好处理时间字段，window函数需要起别名，后面groupby需要用到！ Tumble类来定义滚动窗口！
//      .window(Tumble over 10.seconds on 'pc as 'w)
//      Slide类来定义滑动窗口
      .window(Slide over 10.seconds every 5.seconds on 'pc as 'w)
//      根据id和统一窗口进行分组
      .groupBy('id,'w)
      .select('id, 'id.count())
//      需要定义数据类型，这里简便就用了Row
      .toRetractStream[Row]
      .print()
    env.execute()
  }
}