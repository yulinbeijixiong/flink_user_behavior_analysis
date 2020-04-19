package com.oujian.analysis


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UserWindowResultView(itemId: Long,windEnd: Long, count: Long)

object HotTopN {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //将数据的生产时间设置时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/hot_goods_analysis/src/main/resources/UserBehavior.csv")
     .map(data => {
      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)

    }).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60L), Time.minutes(5L))
      .aggregate(new PvCount(), new WindowResult())
      .keyBy(_.windEnd)
      .process(new ResultSort(3))
      .print()
    env.execute("user behavior to TopN")
  }
}

class PvCount() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, UserWindowResultView,Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[UserWindowResultView]): Unit = {
    out.collect(UserWindowResultView(key,window.getEnd,input.iterator.next()))
  }
}

class ResultSort(topN: Int) extends KeyedProcessFunction[Long, UserWindowResultView, String] {
  private var listView: ListState[UserWindowResultView] = _

  override def open(parameters: Configuration): Unit = {
    listView = getRuntimeContext.getListState(new ListStateDescriptor[UserWindowResultView]("list_user_window_result", classOf[UserWindowResultView]))
  }

  override def processElement(value: UserWindowResultView, ctx: KeyedProcessFunction[Long, UserWindowResultView, String]#Context, out: Collector[String]): Unit = {
    listView.add(value)
    //延迟1s触发
    ctx.timerService().registerEventTimeTimer(value.windEnd + 1L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserWindowResultView, String]#OnTimerContext, out: Collector[String]): Unit = {
    val views = new ListBuffer[UserWindowResultView]
    import collection.JavaConversions._
    for (item <- listView.get()) {
      if(item.itemId==5051027L){
        println(item)
      }

      views += item
    }
    //释放空间
    listView.clear()
    //i1.count > i2.count 表示i1 >i2 如果成立就将i1 放在i2的前面
    val result: ListBuffer[UserWindowResultView] = views.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    val buffer = new StringBuffer()
    buffer.append("时间：").append(new Timestamp(timestamp)).append(":").append().append("\n")
    buffer.append("商品id ->").append("数量->").append("\n")
    for (i <- result) {

      buffer.append(i.itemId).append(" ->").append(i.count).append("\n")
    }
    Thread.sleep(1000)
    ctx.timerService().deleteEventTimeTimer(timestamp+1)
    out.collect(buffer.toString)
  }
}

