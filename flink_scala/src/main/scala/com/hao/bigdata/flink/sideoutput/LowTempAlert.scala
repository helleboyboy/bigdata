package com.hao.bigdata.flink.sideoutput
import com.hao.bigdata.flink.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:14:44
 * @Describe:
 */
object LowTempAlert {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source:DataStream[SensorReading] = env.addSource(new SensorSource)
      .process(new DIYSideOutput)
    source.print() // datastream正常输出
    source.getSideOutput(new OutputTag[String]("lowTemp_")).print() // 侧输出流输出，id值要一致
    env.execute()
  }
  class DIYSideOutput extends ProcessFunction[SensorReading,SensorReading]{
    //定义一个输出标签  泛型为侧输出的数据类型
    lazy val lowTempALertOutput = new OutputTag[String]("lowTemp_")
    override def processElement(value: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]
                               ): Unit = {
      if (value.temperature < 0){
        // 将符合规则的输出到侧输出 。参数为： 1.侧输出变量名   2.侧输出 的输出信息！！！
        ctx.output(lowTempALertOutput,s"id为 ${value.id} 的传感器小于0度！！！")
      }
      out.collect(value)
    }
  }
}
