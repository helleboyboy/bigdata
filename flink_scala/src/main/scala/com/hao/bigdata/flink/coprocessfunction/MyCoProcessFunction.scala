package com.hao.bigdata.flink.coprocessfunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-05:15:22
 * @Describe:
 */
object MyCoProcessFunction {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source:DataStream[String] = env.socketTextStream("192.168.52.12", 8888)
//    val source2:DataStream[String] = env.socketTextStream("192.168.52.12", 8889)
    val source2:DataStream[String] = env.fromElements(
      "a"
    )
    val source1:DataStream[(String,String)] = source.map(data => {
      val datas = data.split(" ")
      (datas(0), datas(1))
    })

    val result:DataStream[(String,String)] = source1.connect(source2)
      .keyBy(_._1,_.concat("1"))
      .process(new MyCoProcessFunctionmessage)
    result.print()
    env.execute()
  }

  class MyCoProcessFunctionmessage extends CoProcessFunction[(String,String),String,(String,String)]{
    //定义一个开关变量 ,boolean的状态变量默认值为false
    lazy val flag = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("flag_",Types.of[Boolean])
    )
    /**
     * @DESCRIBE 针对第一条流的数据！！！value的泛型可以看出来！！！
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement1(value: (String, String),
                                 ctx: CoProcessFunction[(String, String),
                                   String, (String, String)]#Context,
                                 out: Collector[(String, String)]
                                ): Unit = {
      if (flag.value()){ // 如果开关为开启的时候，则发送到下游
        out.collect(value)
      }
    }

    /**
     * @DESCRIBE 针对第二条流的数据！！！value的泛型可以看出来！！！
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement2(value: String,
                                 ctx: CoProcessFunction[(String, String),
                                   String, (String, String)]#Context,
                                 out: Collector[(String, String)]
                                ): Unit = {
      flag.update(true) //更新开关的状态！
      // 控制放行事件为 100s
      val ts:Long = ctx.timerService().currentProcessingTime() + 100 * 1000L
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[(String, String),
                           String, (String, String)]#OnTimerContext,
                         out: Collector[(String, String)]
                        ): Unit = {
      //将开关的状态置为false,即默认值，也可以用update！！！
      flag.clear()
    }
  }
}
