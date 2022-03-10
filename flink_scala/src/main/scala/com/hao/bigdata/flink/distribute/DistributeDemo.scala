package com.hao.bigdata.flink.distribute

import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.{MapFunction, Partitioner}
import org.apache.flink.streaming.api.scala._

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-28:21:32
 * @Describe:
 */
object DistributeDemo {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
    val transform = source
      //      .map(new DiyMap)
      .filter(x => !x.equals("sensor_1"))

    //    transform.shuffle.print()
    //    transform.rebalance.print()
    //    transform.rescale.print()

    //    transform.broadcast.print()
    //    transform.global.print()
    // partitionCustom方法参数： 1.自定义分区对象； 2.分区字段
    transform.partitionCustom(new MyPartitionCustom,0).print()
    env.execute()
  }
  /**
   * 自定义分区策略
   */
  class MyPartitionCustom extends Partitioner[String]{
    val random = scala.util.Random
    // 方法的两个参数：1.key的数据类型； 2.分区总数
    override def partition(key: String, numPartitions: Int): Int = {
      if (key.equals("sensor_1")){
        2
      }else{
        random.nextInt(numPartitions) //numPartitions数字的随机数！！！
      }
    }
  }
  class DiyMap extends MapFunction[SensorReading,String]{
    override def map(value: SensorReading): String = {
      return value.id
    }
  }
}
