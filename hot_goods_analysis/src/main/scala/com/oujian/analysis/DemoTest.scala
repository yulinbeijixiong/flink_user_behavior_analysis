package com.oujian.analysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object DemoTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val unit: DataStream[String] = environment.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/hot_goods_analysis/src/main/resources/test.txt")
    val unit1: DataStream[(String, Long)] = unit.map(data => {


      val array: Array[String] = data.split(" ")
      (array(0),array(1).trim.toLong)

    }
    )
    val value: DataStream[(String, Long, Long)] = unit1.assignAscendingTimestamps(_._2*1000)
      .keyBy(_._1)
      .timeWindow(Time.seconds(6), Time.seconds(2))
      .aggregate(new Count(), new WindowDemo())
    value.print()


    environment.execute()
  }
}
class Count()extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator+1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class WindowDemo()extends WindowFunction[Long,(String,Long,Long),String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long, Long)]): Unit = {
    out.collect((key,input.iterator.next(),window.getEnd))
  }
}
