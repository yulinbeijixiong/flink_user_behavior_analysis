package com.oujian.network

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import collection.JavaConversions._
case class UvBloomResult(windowEnd:Long,count:Long,rmark:String)
object BloomUniqueVisitorDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream: DataStream[String] = env.readTextFile("/Users/annyu/IdeaProjects/flink_user_behavior_analysis/page_network_flow/src/main/resources/UserBehavior.csv")
    dataStream.map(data => {

      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)

    }).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data=>("dumy",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .trigger(new MyUvBloomTrigger())
      .process(new MyUvBloomFunction())
      .print()
    env.execute()
  }
}
class MyUvBloomTrigger()extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
class Bloom(size:Long) extends Serializable{
  private val cap =size
  def hash(value:String,seed:Long): Long ={
   var result =0L
    for(i <- 0 until value.length){
     result=size*seed+value.charAt(i)
    }
    (cap-1) & result
  }
}
class MyUvBloomFunction() extends ProcessWindowFunction[(String,Long),UvBloomResult,String,TimeWindow]{
  lazy val  bloom=new Bloom(1<<29)
  lazy val jedis = JedisUntil.getJedis().getResource()

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvBloomResult]): Unit = {
    val storeKey: String = context.window.getEnd.toString
    val countValue: String = jedis.hget("count",storeKey)
    var count =0L
    //如果countkey 存在则取出值
    if(countValue!=null){
      count=countValue.toLong
    }
    val userId: String = elements.last._2.toString
    val userHash: Long = bloom.hash(userId,61)
    val boolean: lang.Boolean = jedis.getbit(storeKey,userHash)
    if(!boolean){
      jedis.hset("count",storeKey,(count+1).toString)
      jedis.setbit(storeKey,userHash,true)
      out.collect(UvBloomResult(storeKey.toLong,count+1,"11"))
    }else{
      out.collect(UvBloomResult(storeKey.toLong,count,"22"))
    }


  }
}
object JedisUntil{
  def getJedis(): JedisPool ={
    val config = new JedisPoolConfig()
    config.setMaxWaitMillis(10000L)
    val pool = new JedisPool(config, "hadoop100", 6379, 1000, "redis123")
    pool;
  }
}
