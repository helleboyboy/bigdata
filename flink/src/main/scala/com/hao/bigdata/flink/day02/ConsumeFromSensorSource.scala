package com.hao.bigdata.flink.day02

//引入 隐式转换！
import org.apache.flink.streaming.api.scala._

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:18:24
 * @Describe: 读取消费sensorsource产生的数据！！
 */
object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new SensorSource)
    source.print()
    env.execute()
  }
}
