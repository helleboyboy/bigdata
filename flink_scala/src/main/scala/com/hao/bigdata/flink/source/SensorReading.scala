package com.hao.bigdata.flink.source

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:18:05
 * @Describe:
 */
//定义传感器数据规范！
case class SensorReading(id:String,
                         timestamp:Long,
                         temperature:Double)
