package com.mchou.handler

import com.mchou.bean.StartUpLog
import com.mchou.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object DauHandler {

  /*
   *一：同批次去重：先按照日期、mid分组去重
   * @param filterByRedis 根据Redis去重后的数据
   * @return 返回排序后取的第一个结果
   */
  def filterDataByBach(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {
    //a.将数据转换为((date,mid),log)
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedis.map(log =>
      ((log.logDate, log.mid), log)
    )
    /*  dateMidToLogDStream.print()
      println("**********按照日期排序*******")*/
    //b.按照key分组
    val datemidToIterDS: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()
    //组内的value数据按照日期排序，取第一个,方法一：
    /*val value1: DStream[List[StartUpLog]] = datemidToIterDS.map { case ((logDate, mid), iter) =>
      val logs: List[StartUpLog] = iter.toList.sortWith(_.logDate < _.logDate).take(1)
      logs
    }*/
    //    value1.print()
    //    println("********flatmap********8")

    val value2: DStream[StartUpLog] = datemidToIterDS.flatMap { case ((_, _), logiter) =>
      val logs: List[StartUpLog] = logiter.toList.sortWith(_.ts < _.ts).take(1)
      logs
    }
    value2
  }
  /*
   * 二：跨批次去重(根据Redis中的数据进行去重)
   * @param startLogDStream 从Kafka读取的原始数据
   * @return 返回过滤后的数据集
   */
  def filterDataByRedis(startLogDStream: DStream[StartUpLog]) = {
    startLogDStream.transform(rdd => {
      //批量读取Redis数据进行过滤
      rdd.mapPartitions(iter => {
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //过滤:过滤出是为真的数据，所以要加上不是在redis中
        val logs: Iterator[StartUpLog] = iter.filter(log => {
          val redisKey = s"dau${log.logDate}"
          !jedisClient.sismember(redisKey, log.mid)
        })
        jedisClient.close()
        //返回过滤后的数据集
        logs
      })
    })


  }

  /*
   * 将2次过滤后的数据集中的mid保存至Redis
   * @param startlogDS 经过2次过滤后的数据集
   * @return
   */
  def saveMidRedis(startlogDS: DStream[StartUpLog]): Unit = {

    startlogDS.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })
        jedisClient.close()
      })
    })
  }
}
