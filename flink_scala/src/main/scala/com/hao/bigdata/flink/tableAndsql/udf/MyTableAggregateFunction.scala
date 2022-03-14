package com.hao.bigdata.flink.tableAndsql.udf
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-14:21:25
 * @Describe:
 *      暂不支持sql风格！！！
 */
object MyTableAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    // table API
    val table = tEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature, 'pc.proctime)
    val myTableAggregateFunctionAction = new MyTableAggregateFunctionAction
    table
      .groupBy('id)
      .flatAggregate(myTableAggregateFunctionAction('temperature) as ('temp ,'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
  class Top2Acc{
    var firstHighestTemp : Double = Double.MinValue
    var secondHighestTemp : Double = Double.MinValue
  }
  class MyTableAggregateFunctionAction extends TableAggregateFunction[(Double,Int),Top2Acc]{
    override def createAccumulator(): Top2Acc = new Top2Acc

    def accumulate(acc : Top2Acc, temperature : Double): Unit ={
      if (temperature > acc.firstHighestTemp){
        acc.secondHighestTemp = acc.firstHighestTemp
        acc.firstHighestTemp = temperature
      }else if (temperature > acc.secondHighestTemp){
        acc.secondHighestTemp = temperature
      }
    }
    def emitValue(acc : Top2Acc, out : Collector[(Double, Int)]): Unit ={
      out.collect(acc.firstHighestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}
