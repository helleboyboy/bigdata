package com.hao.bigdata.flink.cep
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-09:21:02
 * @Describe:
 */
object TimeOutOrder {
  case class Order(orderId:String, OrderType:String,orderTs:Long)
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream:KeyedStream[Order,String] = env.fromElements(
      Order("order1", "create", 1000L),
      Order("order2", "create", 2000L),
      Order("order3", "pay", 4000L)
    )
      .assignAscendingTimestamps(_.orderTs)
      .keyBy(_.orderId)

    val myPattern = Pattern
      .begin[Order]("create")
      .where(_.OrderType.equals("create"))
      .next("pay")
      .where(_.OrderType.equals("pay"))
      .within(Time.seconds(10))

    val myPatternStream:PatternStream[Order] = CEP.pattern(stream, myPattern)

    val timeoutOrder = new OutputTag[String]("timeout_pay")

    val timeoutFunction = (map:scala.collection.Map[String,Iterable[Order]],ts:Long, out:Collector[String])=>{
      val orderCreate = map("create").iterator.next()
      out.collect("订单id为 : " + orderCreate.orderId + " 在 " + ts + " ms支付失败！ ")
    }

    val paySuccess = (map:scala.collection.Map[String,Iterable[Order]], out :Collector[String]) => {
      val orderPay = map("pay").iterator.next()
      out.collect("订单id为： " + orderPay.orderId + " 支付成功！！！")
    }

    val result = myPatternStream
      .flatSelect(timeoutOrder)(timeoutFunction)(paySuccess)
    result.print()
    result.getSideOutput(timeoutOrder).print()
    env.execute()

  }
}
