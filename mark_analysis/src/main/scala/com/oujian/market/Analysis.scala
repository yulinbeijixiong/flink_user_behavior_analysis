package com.oujian.market

import java.lang
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
case class MarketAndChannelBehavior(userId:Long,channel:String,timestamp:Long,behavior:String)
case class MarketCountResult(windowEnd:Long,behavior:String,channel:String,count:Long)
object Analysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val ds: DataStream[MarketAndChannelBehavior] = env.addSource(new SimulatedEventSource)
    ds.assignAscendingTimestamps(_.timestamp)
      .map(data=> ((data.behavior,data.channel),1L))
      .filter(!_._1._1.equals("Unintall"))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new CountAggr(),new ResultWindowFunction())
      .print()


    env.execute()
  }
}
class SimulatedEventSource extends RichParallelSourceFunction[MarketAndChannelBehavior]{
  private var bool = true
  val channelType: Seq[String] = Seq("WeChat","AppStore","HuaweiStore","GoogleStore","Weibo","Tieba")
  val behaviorType = Seq("Install","Uninstall","Click","Browse","Purchase")
  private val randmon = new Random()
  override def run(ctx: SourceFunction.SourceContext[MarketAndChannelBehavior]): Unit = {
    val maxCount = Long.MaxValue
    var count = 0L
    while (bool && count<maxCount){
      val channel: String = channelType(randmon.nextInt(channelType.size))
      val behavior: String = behaviorType(randmon.nextInt(behaviorType.size))
      val timestamp= System.currentTimeMillis()
      val userId: Long = randmon.nextInt(8).longValue()
      ctx.collect(MarketAndChannelBehavior(userId,channel,timestamp,behavior))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10)
    }
  }

  override def cancel(): Unit = {bool=false}
}
class CountAggr() extends AggregateFunction[((String,String),Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ((String, String), Long), accumulator: Long): Long = accumulator+1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class ResultWindowFunction() extends ProcessWindowFunction[Long,MarketCountResult,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[Long], out: Collector[MarketCountResult]): Unit = {
    out.collect(MarketCountResult(context.window.getEnd,key._2,key._1,elements.iterator.next()))
  }
}