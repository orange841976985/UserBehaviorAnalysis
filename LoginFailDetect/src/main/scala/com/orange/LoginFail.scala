package com.orange

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by:Orange on 2020.5.6,0006
  * 恶意登录监控
  */

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
object LoginFail {

  def main(args: Array[String]): Unit = {
    //环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()
    env.execute("Login Fail Detect Job")
  }

}

class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
  //  定义状态变量
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))
  override def processElement(login: LoginEvent,
                              context: KeyedProcessFunction[Long, LoginEvent,
                                LoginEvent]#Context,
                              out: Collector[LoginEvent]): Unit = {
    // val timerService = context.timerService
    if (login.eventType == "fail") {
      loginState.add(login)
    }
    //  注册定时器，触发事件设定为 2 秒后
    context.timerService.registerEventTimeTimer(login.eventTime + 2 * 1000)
  }
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent,
                         LoginEvent]#OnTimerContext,
                       out: Collector[LoginEvent]): Unit = {
    val allItems: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- loginState.get) {
      allItems += item
      loginState.clear()
      if (allItems.length > 1) {
        out.collect(allItems.head)
      }
    }
  }
}