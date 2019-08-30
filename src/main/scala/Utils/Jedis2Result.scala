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
    lines.map(x=> {
      val time = x._1._1
      val pro = x._1._2
      val counts = x._2.head - x._2(2)
      (time,pro,counts)
    }).foreachPartition(f=>{
      val connection = JdbcMySQL.getConn()
      f.foreach(t=>{
        val sql = "insert into protime(time,pro,counts) values('"+t._1+"', '"+t._2+"',"+t._3+")"
        val statement = connection.createStatement()
        statement.executeUpdate(sql)
      })
      JdbcMySQL.releaseCon(connection)
    })
  }

  /**
    *  指标3
    * @param lines
    */
  def Result03(lines:RDD[(String,List[Double])]):Unit= {
    lines.sortBy(_._2.head,false).map(x=>
      (x._1,(x._2(2)/ x._2.head *100).formatted("%.1f"))
    ).groupByKey().mapValues(x=>x.size).foreachPartition(f=>{
      val connection = JdbcMySQL.getConn()
      f.foreach(x=>{
        val sql = "insert into provinceTopN(pro,success) values('"+x._1+"',"+x._2+")"
        val statement = connection.createStatement()
        statement.executeUpdate(sql)
      })
      JdbcMySQL.releaseCon(connection)
    })
  }

  /**
    * 指标4
    * @param lines
    */
  def Result04(lines:RDD[(String,List[Double])]):Unit= {
    lines.map(x=>{
      CountMoney.change(List(x._1,x._2(1),x._2(4)))
    }).map(x=>(x.map(_._1),x.map(x=>(x._2,x._3))))
      .reduceByKey((list1,list2)=>list1.zip(list2)
        .map(x=>
        (x._1._1  + x._2._1 ,x._1._2 + x._2._2)))
  }

  /**
    *  指标5
    * @param lines
    */
  def Result05(lines:RDD[((String,String),List[Double])])={
    lines.filter(_._1._1 == "201704").map(x=>{
      (x._1,x._2.head - x._2(1),(x._2.head -x._2(1)/ x._2.head *100).formatted("%.2f"))
    }).sortBy(_._2,false).foreachPartition(x=>{
      val connection = JdbcMySQL.getConn()
      x.foreach(x=>{
        val sql = "insert into failTopN(time,pro,failnumber,failrate)value('" + x._1._1 + "','" + x._1._2 + "', + "+x._2.toDouble + ","+ x._3.toDouble + ")"
        val statement = connection.createStatement()
        statement.executeUpdate(sql)
      })
      JdbcMySQL.releaseCon(connection)
    })
  }
}
