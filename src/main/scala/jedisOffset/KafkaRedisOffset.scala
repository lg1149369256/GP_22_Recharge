package jedisOffset

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.lang

import Utils.{Jedis2Result, TimeUtils}
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}



/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(3))
    // 配置参数
    // 配置基本参数
    // 组名
    val load = ConfigFactory.load()
    val groupId = load.getString("groupid")
    // topic
    val topic = load.getString("topicid")
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = load.getString("broker.list")

    //val brokerList = "192.168.111.4:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] = if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )}
    //  获取province数据并广播出去
    val provinceMap: Map[String, String] = ssc.sparkContext.textFile("D://qianfeng/gp22Dmp/day08/充值平台实时统计分析/city.txt")
      .map(x => {
        val arr = x.split(" ")
        (arr(0), arr(1))
      }).collect.toMap
    // 将生成的provinceMap，广播出去
    val provinceBroadcast: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(provinceMap)

    stream.foreachRDD(rdd=>{
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 业务处理
      val baseData = rdd.map(x => JSON.parseObject(x.value()))
        .filter(json=>{json.getString("serviceName")
          .equalsIgnoreCase("reChargeNotifyReq")&&
      json.getString("interFacRst").equals("0000")})
        .map(json => {
          // 业务结果  0000 成功，其它返回错误编码
          val bussinessRst: String = json.getString("bussinessRst")
         // 金额
          val money: Double = if (bussinessRst.equals("0000"))
            json.getDouble("chargefee")else 0.0

          // 充值成功数
          val success: Int = if (bussinessRst.equals("0000")) 1 else 0
          // 充值的开始时间
          val startReqTime: String = json.getString("requestId")
          // 充值的结束时间
          val endReqTime: String = json.getString("receiveNotifyTime")
//
          // 充值时长
          val timeLong: Double = if (bussinessRst.equals("0000"))
            TimeUtils.changeTime(startReqTime, endReqTime) else 0.0
          // 获得省
          val provinceCode: String = json.getString("provinceCode")
          val province: String = provinceBroadcast.value.getOrElse(provinceCode,"NUll")

          //统计充值订单量，充值金额，充值成功数，充值时长)
          (startReqTime,
            province,
            List[Double](1, money, success.toDouble,timeLong))
        }).cache()
     //
    val res1 = baseData.map(x=>(x._1,x._3)).reduceByKey((list1,list2)=>list1.zip(list2).map(x=>x._2+x._1))
      Jedis2Result.Result01(res1)

      // 统计每小时各个省份的充值失败数据量
    val res2 = baseData.map(x=>((x._1.substring(0,8),x._2),x._3)).reduceByKey((list1,list2)=>list1.zip(list2).map(x=>x._2+x._1))
       Jedis2Result.Result02(res2)

    val res3 = baseData.map(x=>(x._2,x._3)).reduceByKey((list1,list2)=>list1.zip(list2).map(x=>x._2+x._1))
       Jedis2Result.Result03(res3)

    val res4 = baseData.map(x=>(x._1.substring(0,10),x._3)).reduceByKey((list1,list2)=>list1.zip(list2).map(x=>x._2+x._1))
      Jedis2Result.Result04(res4)

    val res5 = baseData.map(x=>((x._1.substring(0,8),x._2),x._3)).reduceByKey((list1,list2)=>list1.zip(list2).map(x=>(x._2+x._1)))
      Jedis2Result.Result05(res5)










        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
