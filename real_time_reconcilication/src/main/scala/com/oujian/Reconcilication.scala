package com.oujian

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class OrderLog(orderId: Long, state: String, payId: String, timestamp: Long)

case class ReceiptLog(payId: String, payType: String, timestamp: Long)

case class Warning(payId: String, msg: String)

object Reconcilication {
  private val outputTag = new OutputTag[Warning]("abnormal_out")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orderData: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/real_time_reconcilication/src/main/resources/OrderLog.csv")
    val receiptLogData: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/real_time_reconcilication/src/main/resources/ReceiptLog.csv")
    val orderStream: DataStream[OrderLog] = orderData.map(data => {

      val array: Array[String] = data.split(",")
      OrderLog(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toLong)
    }
    )
    val order: KeyedStream[OrderLog, String] = orderStream.filter(_.payId != "")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLog](Time.seconds(0)) {
        override def extractTimestamp(element: OrderLog) = element.timestamp * 1000L
      })
      .keyBy(_.payId)
    val receiptLog: DataStream[ReceiptLog] = receiptLogData.map(data => {
      val array: Array[String] = data.split(",")
      ReceiptLog(array(0).trim, array(1).trim, array(2).trim.toLong)
    }
    )

    val receipt: KeyedStream[ReceiptLog, String] = receiptLog.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptLog](Time.seconds(0)) {
      override def extractTimestamp(element: ReceiptLog) = {
        element.timestamp * 1000L
      }
    }).keyBy(_.payId)

    val value: DataStream[(OrderLog, ReceiptLog)] = order.connect(receipt)
      .process(new OrderAndReceiptProcessFunction())
    value.print()
    value.getSideOutput(outputTag).print("time out")


    env.execute("Reconcilication$")
  }

  class OrderAndReceiptProcessFunction() extends CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)] {
    lazy val payState = getRuntimeContext.getState(new ValueStateDescriptor[OrderLog]("order_pay_state", classOf[OrderLog]))
    lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptLog]("receipt_state", classOf[ReceiptLog]))

    override def processElement1(value: OrderLog, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#Context, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
      if (receiptState.value() != null) {
          out.collect((value, receiptState.value()))
          // 清空状态
          payState.clear()
          receiptState.clear()


      }
      payState.update(value)
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L)

    }

    override def processElement2(value: ReceiptLog, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#Context, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
      if (payState.value() != null) {
          payState.clear()
          out.collect((payState.value(), value))

      }
      receiptState.update(value)
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L)

    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#OnTimerContext, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
      if (payState.value() != null) {
        ctx.output(outputTag, Warning(payState.value().payId, "receiptState time out"))
      }
      if (receiptState.value() != null) {
        ctx.output(outputTag, Warning(receiptState.value().payId, "pay time out"))
      }
      payState.clear()
      receiptState.clear()
    }
  }

}
