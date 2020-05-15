package com.orange

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by:Orange on 2020.5.7,0007
  *  begin\notFollowedBy\followedBy 表示事件的类型
  *  begin: 事件的起始
  *  next: 紧挨着的事件
  *  followedBy： 在之后的事件（但不一定紧接着）
  *  notNext: 紧挨着没有发生的事件
  *  notFollowedBy: 之后再也没有发生
  *  start\without\follow 部分：为该事件取名字，用于之后提取该阶段匹配到的相关事件
  *  login\item\coupon 部分：返回一个 boolean 类型，通过流中数据来判断是否匹配事件
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    //  定义匹配模式
    val loginFaiPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType=="fail")
      .next("next").where(_.eventType=="fail")
      .within(Time.seconds(2))

    //在KeyBy之后的流中匹配出定义的pattern stream
    val patternStream = CEP.pattern(loginStream,loginFaiPattern)


    //再从pattern stream当中获取匹配的事件流
    val loginFailDataStream =  patternStream.select(
      //数据都保存在pattern里面了     fail,success 指的是可迭代
      ( pattern:scala.collection.Map[String,Iterable[LoginEvent]]
      )=>{
      //
      val begin = pattern.getOrElse("begin",null).iterator.next()
        val next = pattern.getOrElse("next",null).iterator.next()
        (next.userId,begin.ip,next.ip,next.eventType)
    }
    )

    loginFailDataStream.print()
    env.execute("Login Fail Detect job")
  }
}
