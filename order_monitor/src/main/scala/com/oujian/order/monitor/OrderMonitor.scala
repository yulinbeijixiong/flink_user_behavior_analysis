package com.oujian.order.monitor

import java.util

import com.sun.xml.internal.rngom.binary.visitor.PatternFunction
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
case class OrderLog(orderId:Long,state:String,payId:String,timestamp:Long)
case class Warning(orderId:Long,createTime:Long,msg:String)
case class PayFinishOrder(orderId:Long,createTime:Long,payTime:Long)
object OrderMonitor {
  def main(args: Array[String]): Unit = {
    lazy val timeOutOutputTag = new OutputTag[Warning]("time_out_order")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val ds: DataStream[String] = env
      .readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/order_monitor/src/main/resources/OrderLog.csv")
    val dataLog: DataStream[OrderLog] = ds.map(data => {
      val array: Array[String] = data.split(",")
      OrderLog(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toLong)
    }
    )
    val orderStream: KeyedStream[OrderLog, Long] = dataLog.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(60)) {
      override def extractTimestamp(element: OrderLog) = element.timestamp * 1000L
    })
      .keyBy(_.orderId)
    val pattern: Pattern[OrderLog, OrderLog] = Pattern.begin[OrderLog]("begin")
      .where(_.state.equals("create"))
      .followedBy("follow")
      .where(_.state == "pay")
      .within(Time.minutes(10))
    val patterStream: PatternStream[OrderLog] = CEP.pattern(orderStream,pattern)
    val value: DataStream[PayFinishOrder] = patterStream.select(timeOutOutputTag,new OrderPatternTimeoutFunction(),new OrderPatternFunction())

   value.print("支付完成订单")
    value.getSideOutput(timeOutOutputTag).print("支付超时订单")
    env.execute("OrderMonitor$")

  }

}
class OrderPatternTimeoutFunction() extends PatternTimeoutFunction[OrderLog,Warning]{
  override def timeout(pattern: util.Map[String, util.List[OrderLog]], timeoutTimestamp: Long): Warning = {
    val first: OrderLog = pattern.getOrDefault("begin",null).iterator().next()
    Warning(first.orderId,first.timestamp,"order timeout")
  }

}
class OrderPatternFunction() extends PatternSelectFunction[OrderLog,PayFinishOrder]{
  override def select(pattern: util.Map[String, util.List[OrderLog]]): PayFinishOrder = {
    val first: OrderLog = pattern.get("begin").iterator().next()
    val second: OrderLog = pattern.get("follow").iterator().next()
    PayFinishOrder(first.orderId,first.timestamp,second.timestamp)
  }
}