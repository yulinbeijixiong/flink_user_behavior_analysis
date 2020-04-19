package com.oujian.order.monitor

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
case class OrderResult(orderId:Long,msg:String)
object OrderProcessMonitor1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val ds: DataStream[String] = env.socketTextStream("localhost",7777)
    val dataLog: DataStream[OrderLog] = ds.map(data => {
      val array: Array[String] = data.split(",")
      OrderLog(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toLong)
    }
    )
    dataLog.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(60)) {
      override def extractTimestamp(element: OrderLog) = element.timestamp * 1000L
    })
      .keyBy(_.orderId)
      .process(new OrderProcessFunction1).print()
    env.execute()
  }
}
class OrderProcessFunction1 extends KeyedProcessFunction[Long,OrderLog,OrderResult]{
  lazy val isPay=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_pay",classOf[Boolean]))
  lazy val finalPayTime=getRuntimeContext.getState(new ValueStateDescriptor[Long]("final_pay_time",classOf[Long]))
  override def processElement(value: OrderLog, ctx: KeyedProcessFunction[Long, OrderLog, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val bool: Boolean = isPay.value()
    if(value.state=="create"){
      //已经支付过了
      if(bool){
        OrderResult(value.orderId,"pay successsFully")
        isPay.clear()
        finalPayTime.clear()
      }else {
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp*1000L+3*60*1000L)
        //更新最终支付时间
        finalPayTime.update(value.timestamp*1000L+3*60*1000L)
      }
    }else if(value.state=="pay"){
      if(finalPayTime.value()>0){
        val payTime: Long = finalPayTime.value()
        //当前支付时间小于最终支付时间，输出支付成功
        if(payTime> value.timestamp*1000L){
          out.collect(OrderResult(value.timestamp,"order pay success"))
          isPay.clear()
          finalPayTime.clear()
        }
        //如果没有，则是pay先到
      }else {
        //注册一个pay的支付时间的定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp)
        //更新支付状态
        isPay.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if(isPay.value()) {
      out.collect(OrderResult(ctx.getCurrentKey, "not create order info"))
    }else {
      out.collect(OrderResult(ctx.getCurrentKey,"order time out"))
    }
    isPay.clear()
    finalPayTime.clear()
  }
}
