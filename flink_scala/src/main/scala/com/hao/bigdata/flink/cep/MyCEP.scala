package com.hao.bigdata.flink.cep
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-03-08:22:43
 * @Describe:
 */
object MyCEP {
  case class UserData(userId:String, isFail:Boolean, ipAddr:String, eventTime:Long)
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      UserData("user1", true, "192.168.10.20", 1000L),
      UserData("user1", true, "192.168.10.21", 2000L),
      UserData("user1", true, "192.168.10.22", 3000L),
      UserData("user1", false, "192.168.10.23", 4000L),
      UserData("user2", true, "192.168.10.24", 5000L),//时间戳！！！
      UserData("user2", false, "192.168.10.25", 1000L),
      UserData("user2", false, "192.168.10.26", 2000L),
      UserData("user2", true, "192.168.10.27", 3000L),
      UserData("user2", true, "192.168.10.28", 4000L),
      UserData("user2", true, "192.168.10.29", 5000L),
      UserData("user3", false, "192.168.10.30", 1000L),
      UserData("user3", true, "192.168.10.31", 2000L),
      UserData("user3", true, "192.168.10.32", 3000L),
      UserData("user3", true, "192.168.10.33", 4000L)
    )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    val myPattern:Pattern[UserData,UserData] = Pattern
      .begin[UserData]("first")//第一个事件的名字
      .where(_.isFail.equals(true))//第一个事件符合的条件筛选
      .next("second")//第二个事件的名字
      .where(_.isFail.equals(true))//第二个事件符合的条件筛选
      .next("third")
      .where(_.isFail.equals(true))
      .within(Time.seconds(5L)) // 五分钟之内发生的，从begin开始计算

    val patterStream:PatternStream[UserData] = CEP.pattern[UserData](
      stream,
      myPattern
    )
    // Map[param1，param2] ：第一个参数为key，第二个参数为流
    val result:DataStream[String] = patterStream.select(
      // 匿名函数！！！
      (pattern: collection.Map[String, Iterable[UserData]]) => {
      val first = pattern("first").iterator.next() // 不是 调用get方法！！！
      val second = pattern("second").iterator.next()
      val third = pattern("third").iterator.next()

      "用户" + first.userId + "连续三次登录失败，登录失败的IP为： " + first.ipAddr + " , " + second.ipAddr + " , " + third.ipAddr + "."
    }
    )
    result.print()
    env.execute()
  }
}