package com.oujian.market

import oracle.jrockit.jfr.events.ValueDescriptor
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 广告点击行为
 *
 * @param userId
 * @param adId
 * @param province
 * @param city
 * @param timestamp
 */
case class AdClickBehavior(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdCountResult(windowEnd: Long, province: String, count: Long)
case class BlackListWarning(userId:Long,adId:Long,msg:String)

object AdClickAnalysis {
  private var backList = new OutputTag[BlackListWarning]("black_list")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val adClickData: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/mark_analysis/src/main/resources/AdClickLog.csv")
    val adClickBehavior: DataStream[AdClickBehavior] = adClickData.map(data => {
      val arrayData: Array[String] = data.split(",")
      AdClickBehavior(arrayData(0).trim.toLong, arrayData(1).trim.toLong, arrayData(2).trim, arrayData(3).trim, arrayData(4).trim.toLong)
    })
    val value: DataStream[AdClickBehavior] = adClickBehavior.assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(logData => (logData.userId, logData.adId))
      .process(new FilterBlackList(100))
    value
      .map(data => (data.province, 1))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new AdCountAggr(), new CountResultWindow())
      .print()
    //通过侧输出流输出黑名单
    value.getSideOutput(backList).print()

    env.execute("AdClickAnalysis$")

  }

  class FilterBlackList(maxClick: Int) extends KeyedProcessFunction[(Long, Long), AdClickBehavior, AdClickBehavior] {
    //保存点击数
    lazy val clickCount = getRuntimeContext.getState(new ValueStateDescriptor[Long]("click_count", classOf[Long]))
    //是否发送过黑名单了
    lazy val sentBlackClick = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("sent_black_click", classOf[Boolean]))
    //黑名单重置时间
    lazy val resetTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset_time", classOf[Long]))
    //处理每个元素
    override def processElement(value: AdClickBehavior, ctx: KeyedProcessFunction[(Long, Long),
      AdClickBehavior, AdClickBehavior]#Context, out: Collector[AdClickBehavior]): Unit = {
      val curCount: Long = clickCount.value()
      //如果是第一次注册定时器
      if (curCount == 0) {
        //获取第二天的初始时间
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24L) + 1) * (1000 * 60 * 60 * 24L).toLong
        //更新重置时间
        resetTime.update(ts)
        //注册定时器
        ctx.timerService().registerProcessingTimeTimer(ts)
      }
      if(curCount>maxClick){
        if(!sentBlackClick.value()){
          sentBlackClick.update(true)
          //发送黑名单
          ctx.output(backList,BlackListWarning(value.userId,value.adId,"这个用户点击超过最大次数"))
        }
        return
      }
      clickCount.update(curCount+1)
      out.collect(value)
    }
    //清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickBehavior, AdClickBehavior]#OnTimerContext, out: Collector[AdClickBehavior]): Unit = {
      clickCount.clear()
      sentBlackClick.clear()
      resetTime.clear()
    }
  }


}

class AdCountAggr() extends AggregateFunction[(String, Int), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class CountResultWindow() extends ProcessWindowFunction[Long, AdCountResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[AdCountResult]): Unit = {
    out.collect(AdCountResult(context.window.getEnd, key, elements.iterator.next()))
  }
}