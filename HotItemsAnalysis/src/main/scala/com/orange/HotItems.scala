package com.orange

import java.lang
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable.ListBuffer

/**
  * Created by:Orange on 2020.5.5,0005
  * 实时热门商品统计模块
  * 1.充抽取出业务时间戳,用业务时间做窗口
  * 2.过滤点击行为
  * 3.滑动窗口
  * 4.按每个窗口进行聚合,输出每个窗口中点击量前N名的窗口
  */

/**
  *输入数据样例类
  * @param userId 加密后的用户ID
  * @param itemId 加密后的商品ID
  * @param categoryId 加密后的商品所属类别
  * @param behavior 用户行为,pv:浏览量 buy:商品购买 cart:加购物车 fav:收藏
  * @param timestamp
  */
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

/**
  * 输出数据样例类
  * @param itemId 主键商品ID
  * @param windowEnd  计算的窗口
  * @param count 点击量
  */
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {




  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.100.8:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显示定义time的类型,默认是processingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    val stream = env
//      .readTextFile("I:\\idea\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        .addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(line=>{
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong,linearray(1).toLong,linearray(2).toInt,linearray(3),linearray(4).toLong)
      })
      //js时间戳精确到毫秒,所以要乘以1000后转换
      .assignAscendingTimestamps(_.timestamp*1000) //真实业务一般都是乱序的,所以一般不用assignAscendingTimestamps,而使用BoundedOutOfOrdernessTimestampExtractor。这里我们把每条数据的业务时间当做水位线
      //过滤出pv的所有数据,令:pv及浏览量
      .filter(_.behavior=="pv")
      //根据商品ID进行分组
      .keyBy("itemId")
      //设置滑动窗口,第一个参数:窗口大小1小时,第二个参数:滑动距离五分钟.意思就是统计一个小时里,每五分钟的数据
      .timeWindow(Time.days(1), Time.minutes(5))
      //窗口聚合 new CountAgg()定义窗口聚合规则,new WindowResultFunction()定义输出数据结构
      .aggregate(new CountAgg(),new WindowResultFunction())
      //按窗口进行分组
        .keyBy("windowEnd")
        .process(new TopNHotItems(3))//求点击量前三名的商品




    stream.print()
    env.execute("hot items job")
  }
}
//自定义实现聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long] {
  //迭代状态的初始值
  override def createAccumulator(): Long = 0L
  //每一条输入数据，和迭代数据如何迭代
  override def add(in: UserBehavior, acc: Long): Long = acc+1
  //多个分区的迭代数据如何合并
  override def getResult(acc: Long): Long = acc
  //返回数据，对最终的迭代数据如何处理，并返回结果。
  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

//自定义实现windowFunction输出的是
//in:输入累加器类型,out累加后输出类型,key tuple泛型,w聚合的窗口 w.getRnd可以拿到窗口的结束时间
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long],
                     collector: Collector[ItemViewCount]) : Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {
  private var itemState:ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //命名状态变量的名字和状态变量的类型
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemSate-state",classOf[ItemViewCount])
    //定义状态变量
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(i)
    //注册windowEnd+1的eventTime timer,当触发时,说明收齐了属于windowEnd窗口的所有商品数据
    //也就是当程序看到windowEnd+1的水位线watermark时,触发onTimer回调函数
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取收到的所有商品点击量
    val allItems:ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems+=item
    }
    //提前清除状态中的数据,释放空间
    itemState.clear()
    //按照点击量从大到小排序
    val  sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //将排名信息格式化成string,便于打印
    val result:StringBuilder = new StringBuilder
    result.append("===========================\n")
    result.append(" 时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g. No1 ： 商品 ID=12224 浏览量 =2413
      result.append("No").append(i+1).append(":")
        .append(" 商品 ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    //  控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }

}





