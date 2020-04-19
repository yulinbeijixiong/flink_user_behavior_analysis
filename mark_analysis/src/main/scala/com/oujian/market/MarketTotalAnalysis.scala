package com.oujian.market

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
case class MarketTotalResult(windowEnd:Long,count:Long)
object MarketTotalAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val ds: DataStream[MarketAndChannelBehavior] = env.addSource(new SimulatedEventSource)
    ds.assignAscendingTimestamps(_.timestamp)
        .map(data=>("total",1L))
        .keyBy(_._1)
        .timeWindow(Time.hours(1),Time.seconds(10))
        .process(new TotalMarketProcess())
        .print()
    env.execute("MarketTotalAnalysis")

  }
}
class TotalMarketProcess()extends ProcessWindowFunction[(String,Long),MarketTotalResult,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[MarketTotalResult]): Unit = {
    out.collect(MarketTotalResult(context.window.getEnd,elements.size))
  }
}