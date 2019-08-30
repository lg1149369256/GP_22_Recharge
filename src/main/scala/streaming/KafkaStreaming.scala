package streaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object KafkaStreaming {
  def main (args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("KafkaStreaming")
      .master("local[2]")
      .getOrCreate()

    val load  = ConfigFactory.load()
    // 获取组id：groupd
    val groupId = load.getString("groupid")
    // 获取topicId
    val topicid = load.getString("topicid")
    // 指定Kafka的broker地址
    val brokerlist = load.getString("broker.list")






  }
}
