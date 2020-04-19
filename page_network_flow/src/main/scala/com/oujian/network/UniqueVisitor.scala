package com.oujian.network



import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



object UniqueVisitor {
  def main(args: Array[String]): Unit = {
   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
   env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   env.setParallelism(1)
   val dataStream: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/page_network_flow/src/main/resources/UserBehavior.csv")
   dataStream.map(data => {

    val dataArray: Array[String] = data.split(",")
    UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)

   }).assignAscendingTimestamps(_.timestamp * 1000)
     .filter(_.behavior == "pv")
     .timeWindowAll(Time.seconds(60))
     .apply(new UvCountWindowFunction())
     .print()
   env.execute()
  }
}
class UvCountWindowFunction() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{

 override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
  import collection.JavaConversions._

  val s: collection.mutable.Set[Long] = collection.mutable.Set()

  for(user <- input){
   s += user.userId
  }
  out.collect(UvCount(window.getEnd,s.size) )
 }
}

case class UvCount(getEnd: Long, size: Int)