package com.orange

import java.sql.Timestamp

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by:Orange on 2020.5.8,0008
  * 支付订单实时监控模块
  * CEP实现
  */

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //数据引入
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1,"create",1558430842),
      OrderEvent(2,"create",1558430843),
      OrderEvent(2,"pay",1558436842),
      OrderEvent(2,"pay",1558430844)
    )).assignAscendingTimestamps(_.eventTime*1000)

    //定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType=="create")
      .followedBy("follow").where(_.eventType=="pay")
      .within(Time.minutes(15))

    //定义一个输出标签,用于标明侧输出流,因为我想拿到的最终要的数据是不匹配的数据,而是超时的数据
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")

    //从keyBy知乎每条流数据中匹配定义好的模式,得到一个pattern stream
    //在keyBy之后的流中匹配出定义的pattern stream
    val patternStream = CEP.pattern(orderEventStream,orderPayPattern)

    //再从pattern stream当中获取匹配的事件流,select方法传入一个pattern select function,当检测到定义好的模式序列时就
    import scala.collection.Map
    val completedResultDataStream = patternStream.select(orderTimeoutOutput)(
      //对于超时的序列部分,调用pattern timeout function
      (pattern:Map[String,Iterable[OrderEvent]],timestamp:Long)=>{
      val timeoutOrderId = pattern.getOrElse("begin",null).iterator.next().orderId
        OrderResult(timeoutOrderId,"timeout")
    }
    )(
      //正常匹配的部分,调用pattern select function
      (pattern:Map[String,Iterable[OrderEvent]])=>{
        val payedOrderId = pattern.getOrElse("follow",null).iterator.next().orderId
        OrderResult(payedOrderId,"success")
      }
    )

    //打印匹配的时间序列
    completedResultDataStream.print()

    //拿到同一标签的timeout匹配的结果(流)
    val timeoutResult = completedResultDataStream.getSideOutput(orderTimeoutOutput)
    timeoutResult.print()
    env.execute("Order Timeout Detect Job")



      }

}
