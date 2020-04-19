package com.oujian.login

import java.lang

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginLog(userId: Long, ip: String, state: String, timestamp: Long)
case class Warning(userId:Long,firstTime:Long,lastTime:Long,count:Long,msg:String)
object LoginFailDetect {
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
            element.timestamp*1000L
          }
        })
      .keyBy(_.userId)
      .process(new LoginLogFailFunction(2))
        .print()
    env.execute("LoginFailDetect$")


  }
}
class  LoginLogFailFunction(maxLoginFail: Int) extends  KeyedProcessFunction[Long,LoginLog,Warning]{
  //用于储存登录失败的状态
  lazy val loginFailState=getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("login_fail_state",classOf[LoginLog]))
  lazy val firstLoginFailTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("login_fail_time",classOf[Long]))
  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, Warning]#Context, out: Collector[Warning]): Unit = {
    val loginFailIter: lang.Iterable[LoginLog] = loginFailState.get()
    //判断是否为登录失败
    if(value.state=="fail"){
      //判断是否有登录失败的状态，没有就是第一次
      if(!loginFailIter.iterator().hasNext){
        //注册定时器
        ctx.timerService().registerEventTimeTimer((value.timestamp+2)*1000L)
        //保存定时器触发时间
        firstLoginFailTime.update((value.timestamp+2)*1000L)
      //不是第一次就更新状态
      }
      loginFailState.add(value)
      //登录成功清空状态
    }else{
      loginFailState.clear()
      firstLoginFailTime.clear()
      //删除定时器
       ctx.timerService().deleteEventTimeTimer(firstLoginFailTime.value())
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    val logs: lang.Iterable[LoginLog] = loginFailState.get()
    val listBufferLogs = new ListBuffer[LoginLog]
    import collection.JavaConversions._
    for (log<-logs){

      listBufferLogs += log
    }
    loginFailState.clear()
    if(listBufferLogs.size>=maxLoginFail){
      out.collect(Warning(listBufferLogs.head.userId,listBufferLogs.head.timestamp,listBufferLogs.last.timestamp,listBufferLogs.size,"login fail warning"))
    }
  }
}
