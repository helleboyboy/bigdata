package com.hao.bigdata.flink.tableAndsql.udf
import com.hao.bigdata.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:21:46
 * @Describe:
 *    1.标量函数的自定义实现
 *    2.自定义函数需要继承ScalarFunction类，然后自定义方法，但是方法名必须为eval
 *    3.函数必须注册在tableEnvironment中
 *    4.继承SclarFunction类的类名不一定必须为UDF方法名，UDF方法名为registerFunction方法的第一个参数！！！
 */
object MyScalarFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    // 需要调用重写ScalarFucntion类，注册到tEnv
//    val hashCode_Udf = new HashCode_Udf(5)
//    tEnv.registerFunction("hashCode_Udf", hashCode_Udf)
//    val table = tEnv.fromDataStream(sourceDataStream)
//    table.select('id, hashCode_Udf('id))
//      .toRetractStream[Row]
//      .print()
    //上面是Table API

    //下面是sql风格！
    val hashCode_Udf = new HashCode_Udf(5)
    tEnv.registerFunction("hashCode_Udf1", hashCode_Udf)
    tEnv.createTemporaryView("sensor_udf", sourceDataStream)
//    registerFunction的别名必须用在sql风格上，否则会无法辨别出split_tableFunction！！
    val sqlString = "SELECT id, hashCode_Udf1(id) FROM sensor_udf"
    tEnv.sqlQuery(sqlString)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
  class HashCode_Udf(input: Int) extends ScalarFunction{
    def eval(str : String): Int ={
      str.hashCode * input
    }
  }
}