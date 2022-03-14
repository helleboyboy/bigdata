package com.hao.bigdata.flink.tableAndsql.udf
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-14:0:09
 * @Describe:
 *    step:
 *      1.将流转换为表
 *      2.像tEnv注册函数
 *      3.使用函数！
 */
object MyAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.addSource(new SensorSource).filter(_.id.equals("sensor_2"))
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    // Table API
//    val table = tEnv.fromDataStream(sourceDataStream, 'id, 'timestamp as 'ts, 'temperature, 'pc.proctime)
//    val aggregateAction = new MyAggregateFunctionAction()
//    table
//      .groupBy('id)
//      .aggregate(aggregateAction('temperature) as 'avgTemp)
//      .select('id, 'avgTemp)
//      .toRetractStream[Row]
//      .print()

//    // sql风格
    tEnv.createTemporaryView("sensor_aggre",sourceDataStream, 'id, 'timestamp as 'ts, 'temperature,'pc.proctime)
    tEnv.registerFunction("avgTemp", new MyAggregateFunctionAction)
    val sqlString =
      """
        |SELECT
        |id, avgTemp(temperature)
        |FROM
        |sensor_aggre
        |GROUP BY id
        |""".stripMargin
    tEnv.sqlQuery(sqlString)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
  class AvgTempAcc{
    var sum : Double = 0.0
    var count : Int = 0
  }
  // 返回的数据类型
  //第一个为温度值的类型，第二个为累加器类型
  class MyAggregateFunctionAction extends AggregateFunction[Double, AvgTempAcc]{
    override def getValue(acc: AvgTempAcc): Double = {
      acc.sum / acc.count
    }

    def accumulate(acc : AvgTempAcc, temperature : Double): Unit ={
      acc.sum += temperature
      acc.count += 1
    }

    override def createAccumulator(): AvgTempAcc = {
      new AvgTempAcc
    }
  }
}
