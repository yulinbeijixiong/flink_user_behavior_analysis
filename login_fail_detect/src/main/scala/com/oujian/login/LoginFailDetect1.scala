package com.oujian.login

import java.lang

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailDetect1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/login_fail_detect/src/main/resources/LoginLog.csv")
    val loginLogDs: DataStream[LoginLog] = ds.map(data => {

      val dataArray: Array[String] = data.split(",")
      LoginLog(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
    loginLogDs
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(1)) {
        override def extractTimestamp(element: LoginLog) = {
          element.timestamp * 1000L
        }
      })
      .keyBy(_.userId)
      .process(new LoginFailProcessFunction())
      .print()
    env.execute("login fail detect")
  }
}

class LoginFailProcessFunction() extends KeyedProcessFunction[Long, LoginLog, Warning] {
  lazy val loginFailList = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("login_fail", classOf[LoginLog]))

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, Warning]#Context, out: Collector[Warning]): Unit = {
    val logs: lang.Iterable[LoginLog] = loginFailList.get()
    if (value.state == "fail") {

      if (logs.iterator().hasNext) {
        //登录失败并且间隔时间小于2s
        val log: LoginLog = logs.iterator().next()
        if (log.timestamp - value.timestamp < 2L) {
          out.collect(Warning(log.userId, log.timestamp, value.timestamp, 2, "login fail abnormal"))

        }

        loginFailList.clear()
//        loginFailList.add(value￿￿￿)
      }
      loginFailList.add(value)
    }else{
      loginFailList.clear()
    }
  }
}
