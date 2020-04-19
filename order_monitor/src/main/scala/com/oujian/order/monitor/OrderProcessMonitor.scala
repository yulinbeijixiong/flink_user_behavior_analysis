package com.oujian.order.monitor

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderProcessMonitor {
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
    val orderStream: KeyedStream[OrderLog, Long] = dataLog.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(60)) {
      override def extractTimestamp(element: OrderLog) = element.timestamp * 1000L
    })
      .keyBy(_.orderId)
    orderStream.process(new OrderProcessFunction()).print()
    env.execute("OrderProcessMonitor$")

  }
}
class OrderProcessFunction() extends  KeyedProcessFunction[Long,OrderLog,Warning]{
  lazy val isPay=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_pay",classOf[Boolean]))
  override def processElement(value: OrderLog, ctx: KeyedProcessFunction[Long, OrderLog, Warning]#Context, out: Collector[Warning]): Unit = {
    val bool: Boolean = isPay.value()
    //支付数据先到的情况
    if(bool){
      isPay.clear()
    }else{

      if(value.state=="create"){
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp*1000L+3*60*1000L)
        //支付数据前到
      }else if(value.state=="pay"){
        isPay.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    val bool: Boolean = isPay.value()
    if(!bool){
      out.collect(Warning(ctx.getCurrentKey,timestamp-3*60*1000L,"order time out"))
    }
  }
}
