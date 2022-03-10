package com.hao.bigdata.flink.richfunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:22:27
 * @Describe:
 */
object RichFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source:DataStream[String] = env.fromElements("hello world", "hello flinki", "welcome to org.apache.flink")
    source.map(new MYDIYRichFunction).print()
    env.execute()
  }

  class MYDIYRichFunction extends RichMapFunction[String,String]{
    override def open(parameters: Configuration): Unit = {
      println("程序的第一个调用方法是我")
    }

    override def map(value: String): String = {
      val taskName = getRuntimeContext.getTaskName
      "任务名字为： " + taskName
    }

    override def close(): Unit = {
      println("喔是程序的最后一个被调用的方法！！！")
    }
  }



}

