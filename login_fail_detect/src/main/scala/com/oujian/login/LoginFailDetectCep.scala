package com.oujian.login


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailDetectCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataDs: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/login_fail_detect/src/main/resources/LoginLog.csv")
    val loginLogDs: DataStream[LoginLog] = dataDs.map(
      data => {

        val array: Array[String] = data.split(",")
        LoginLog(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toLong)
      }

    )
    val loginLogKeyStream: KeyedStream[LoginLog, Long] = loginLogDs.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(0)) {
      override def extractTimestamp(element: LoginLog) = element.timestamp * 1000L
    }).keyBy(_.userId)

    val pattern: Pattern[LoginLog, LoginLog] = Pattern.begin[LoginLog]("begin")
      .where(_.state == "fail")
      .next("next")
      .where(_.state == "fail")
      .within(Time.minutes(10))

    val patternStream: PatternStream[LoginLog] = CEP.pattern( loginLogKeyStream,pattern)
    patternStream.select((patternSelectFunction:collection.Map[String,Iterable[LoginLog]])=>{
      val first: LoginLog = patternSelectFunction.getOrElse("begin",null).iterator.next()
      val second: LoginLog = patternSelectFunction.getOrElse("next",null).iterator.next()

      Warning(first.userId,first.timestamp,second.timestamp,2,"wraning")
    }).print()
    env.execute("LoginFailDetectDep$")

  }
}
