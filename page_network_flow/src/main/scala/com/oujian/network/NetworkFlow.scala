package com.oujian.network

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheNetworkFlow(ip: String, simpleTime: Long, url: String)
case class WindowResultView(url:String,windowEnd:Long,count:Long)
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val logData: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/page_network_flow/src/main/resources/apache.log")
    val dataApacheNetworkFlow: DataStream[ApacheNetworkFlow] = logData.map(data => {
      val arrayLog: Array[String] = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time: Long = format.parse(arrayLog(3)).getTime
      ApacheNetworkFlow(arrayLog(0), time, arrayLog(6))
    })
    //设置水印
     dataApacheNetworkFlow.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheNetworkFlow](Time.seconds(0)) {
      override def extractTimestamp(element: ApacheNetworkFlow) = {
        element.simpleTime
      }
    }).keyBy(_.url)
      .timeWindow(Time.minutes(60L), Time.minutes(5))
      //允许延迟一分钟的数据
      .allowedLateness(Time.minutes(1))
      .aggregate(new CountAggr(), new WindowResult())
      .keyBy(_.windowEnd)

      .process(new ViewPageTopN(3))

      .print()
    env.execute()

  }
}
class CountAggr()extends AggregateFunction[ApacheNetworkFlow,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheNetworkFlow, accumulator: Long): Long = {
    accumulator+1L
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class WindowResult()extends WindowFunction[Long,WindowResultView,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[WindowResultView]): Unit = {
    out.collect(WindowResultView(key,window.getEnd,input.iterator.next()))
  }
}
class ViewPageTopN(i: Int)extends KeyedProcessFunction[Long,WindowResultView,String]{
  private var listState:ListState[WindowResultView] =_

  override def open(parameters: Configuration): Unit = {
    listState=getRuntimeContext.getListState( new ListStateDescriptor[WindowResultView]("view_page_list",classOf[WindowResultView]))
  }
  override def processElement(value: WindowResultView, ctx: KeyedProcessFunction[Long, WindowResultView, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, WindowResultView, String]#OnTimerContext, out: Collector[String]): Unit = {
    val pageTopNs = new ListBuffer[WindowResultView]()
    import collection.JavaConversions._
    for(p<-listState.get()){
      pageTopNs +=p
    }
    listState.clear()
    val views: ListBuffer[WindowResultView] = pageTopNs.sortWith((x1,x2)=>x1.count>x2.count).take(5)
    val buffer = new StringBuffer()

    buffer.append("页面TopN 时间：").append(new Timestamp(timestamp)).append("\n")
    buffer.append("页面  ->").append("数量").append("\n")

    for(v <-views){
      buffer.append(v.url).append(" ->").append(v.count).append("\n")
    }
    out.collect(buffer.toString)
  }

}

