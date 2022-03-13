package com.hao.bigdata.flink.tableAndsql.udf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
/**
 *
 * @Author: Java页大数据
 * @Date: 2022-03-13:22:27
 * @Describe:
 *    注意事项：
 *      1.先实现继承TableFunction类，实现eval方法，注意泛型
 *      2.注册表函数之前，需要实现自定义类的使用
 *      3.registerFunction方法的第一个参数为 方法名，其用在SQL风格上，而在Table API上则是无效的
 *      4.T为flink的固定语法，其定义一个元组类型！！！
 *      5.尽量早点为表的字段进行标注！！！
 */
object MyTableFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream = env.fromElements(
      "hello;world",
      "hello;flink",
      "hello;java 页大数据",
      "hello;bigdata"
    )
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val split_tableFunction = new Split(";")
    // mySplit为名字,必须用在sql风格上，用在Table API无效！！！ 名字为split_tableFunction
//    tEnv.registerFunction("mySplit",split_tableFunction)
//    tEnv.fromDataStream(sourceDataStream, 's)
////      字符串和其切割开的字符串进行join。可以看到结果为s列的值存在多行！！！
//      .joinLateral(split_tableFunction('s) as ('word, 'wordLen))
//      .select('s, 'word, 'wordLen)
//      .toRetractStream[Row]
//      .print()
//    上面为table API

//    下面为SQL风格
    tEnv.registerFunction("mySplit", split_tableFunction)
    tEnv.createTemporaryView("sensor_table", sourceDataStream, 's)
    /**
     * 注意项：
     *    1.registerFunction的别名必须用在sql风格上，否则会无法辨别出split_tableFunction！！
     *    2.T标注为元组，其为flink的固定语法！
     */
    val sqlString = "SELECT s, word, wordLen FROM sensor_table, LATERAL TABLE(MySplit(s)) as T(word, wordLen)"
    tEnv.sqlQuery(sqlString)
      .toRetractStream[Row]
      .print()
    env.execute()
  }
  // TableFunction需要指明参数，即表函数返回的数据类型
  class Split(sep : String) extends TableFunction[(String, Int)]{
    def eval(str : String): Unit ={
      str.split(sep).foreach(x => {
        collect((x,x.length))
      })
    }
  }
}