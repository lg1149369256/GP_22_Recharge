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
          val rechargeRes = json.getString("bussinessRst") // 充值结果
          val fee: Double = if (rechargeRes.equals("0000")) // 判断是否充值成功
            json.getDouble("chargefee") else 0.0 // 充值金额
          val feeCount = if (!fee.equals(0.0)) 1 else 0 // 获取到充值成功数,金额不等于0
          val starttime = json.getString("requestId") // 开始充值时间
          val recivcetime = json.getString("receiveNotifyTime") // 结束充值时间
          val pcode = json.getString("provinceCode") // 获得省份编号
          val province = provinceBroadcast.value.get(pcode).toString // 通过省份编号进行取值
          // 充值成功数
          val isSucc = if (rechargeRes.equals("0000")) 1 else 0
          // 充值时长
          val costtime = if (rechargeRes.equals("0000")) TimeUtils.changeTime(starttime, recivcetime) else 0

          (starttime.substring(0, 8), // 年月日
            starttime.substring(0, 10), // 年月日时
            List[Double](1, fee, isSucc, costtime.toDouble, feeCount), // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
            province, // 省份
            starttime.substring(0, 12), // 年月日时分
            (starttime.substring(0, 10), province) // (年月日时，省份)
          )}).cache()

      // 指标一
      // 要将两个list拉倒一起去，因为每次处理的结果要合并
      val result1 = baseData.map(t => (t._1, t._3)).reduceByKey((list1, list2) => {
        // 拉链操作
        list1.zip(list2).map(t => t._1 + t._2)})
      Jedis2Result.Result01(result1)

      // 指标二
      val result2 = baseData.map(t => (t._6, t._3)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)})
      Jedis2Result.Result02(result2)

      // 指标三
      val result3 = baseData.map(t => (t._4, t._3)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)})
      Jedis2Result.Result03(result3)

      // 指标四
      // 要将两个list拉倒一起去，因为每次处理的结果要合并
      val result4 = baseData.map(t => (t._5, t._3)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)})
      Jedis2Result.Result04(result4)

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
