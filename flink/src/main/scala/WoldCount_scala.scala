import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 * Create with idea
 *
 * @Author: Java页大数据
 * @Date: 2022-02-27:2:01
 * @Describe:
 */
object WoldCount_scala {

  case class WordWithCount(word:String,count:Int)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

//    env.setParallelism(2)
    val source = env.socketTextStream("leader", 8888)
    val result = source.flatMap(line => line.split("\\s"))
      .map(word => (word,1))
      .keyBy(0)
      .timeWindow(Time.milliseconds(1L))
      .sum(1)

    result.print()

    env.execute()
  }

}
