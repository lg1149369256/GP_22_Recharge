package Utils


import jedisOffset.JedisConnectionPool
import org.apache.spark.rdd.RDD

/**
  * 业务结果调用接口
  */
object Jedis2Result {
  /**
    *  指标1
    */
  def Result01(lines:RDD[(String,List[Double])]):Unit={
    lines.foreachPartition(f=>{
    val jedis = JedisConnectionPool.getConnection()
    f.foreach(t=>{
      // 充值总数
       jedis.hincrBy(t._1.substring(0,8),"total",t._2(0).toLong)
      // 充值金额
      jedis.hincrByFloat(t._1.substring(0,8),"money",t._2(1))
      // 充值成功数
      jedis.hincrBy(t._1.substring(0,8),"success",t._2(2).toLong)
      // 充值时长
      jedis.hincrBy(t._1.substring(0,8),"time",t._2(3).toLong)
    })
    jedis.close()
  })
  }

  /**
    *  指标2
    */
  def Result02(lines:RDD[((String,String),List[Double])]):Unit={
    lines.foreachPartition(f => {
      val connection = JdbcMySQL.getConn()
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      f.foreach(t => {
        val sql ="insert into protime(protime,counts) values('"+t._1+"',"+(t._2.head -t._2(2))+")"
       val state = connection.createStatement()
        state.executeUpdate(sql)
      })
      JdbcMySQL.releaseCon(connection)
    })
  }

  /**
    *  指标3
    * @param lines
    */
  def Result03(lines:RDD[(String,List[Double])]):Unit= {
    lines.sortBy(_._2.head, false)
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      .map(t => (t._1, (t._2(2) / t._2.head * 100).formatted("%.1f")))
      .foreachPartition(t => {
        // 拿到连接
        val conn = JdbcMySQL.getConn()
        t.foreach(t => {
          val sql = "insert into provinceTopN(pro,success)values('" + t._1 + "'," + t._2.toDouble + ")"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMySQL.releaseCon(conn)
      })
  }

  /**
    * 指标4
    * @param lines
    */
  def Result04(lines:RDD[(String,List[Double])]):Unit= {
    lines.map(t => {
      CountMoney.change(List(t._1, t._2(1), t._2(4)))
    }).map(t => (t.map(_._1), t.map(t => (t._2, t._3))))
      .reduceByKey((list1, list2) => list1.zip(list2)
        .map(t => (t._1._1 + t._2._1, t._1._2 + t._2._2)))
      .foreachPartition(t => {
        // 拿到连接
        val conn = JdbcMySQL.getConn()
        t.foreach(t => {
          val sql = "insert into RealTime(hour,count,money)values('" + t._1 + "'," + t._2.map(_._1.toDouble) + "," + t._2.map(_._2.toDouble) + ")"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMySQL.releaseCon(conn)
      })

  }

  /**
    *  指标5
    * @param lines
    */
  def Result05(lines:RDD[((String,String),List[Double])])={
//    lines.filter(_._1._1 == "20170412").map(x=>{
//      (x._1,x._2.head - x._2(1),(x._2.head -x._2(1)/ x._2.head *100).formatted("%.2f"))
//    }).sortBy(_._2,false).foreachPartition(x=>{
//      val connection = JdbcMySQL.getConn()
//      x.foreach(x=>{
//        val sql = "insert into failTopN(time,pro,failnumber,failrate)values('" + x._1._1 + "','" + x._1._2 + "',"+x._2.toDouble + ","+ x._3.toDouble + ")"
//        val statement = connection.createStatement()
//        statement.executeUpdate(sql)
//      })
//      JdbcMySQL.releaseCon(connection)
//    })
  }
}
