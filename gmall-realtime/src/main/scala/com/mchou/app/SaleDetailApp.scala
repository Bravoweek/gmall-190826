package com.mchou.app

import java.util

import com.alibaba.fastjson.JSON
import com.mchou.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.mchou.constants.GmallConstants
import com.mchou.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    val ssc = new StreamingContext(conf, Seconds(3))

    //读取3个kafka数据创建流
    val orderInfokafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_TOPIC))
    val orderDetailkafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
    val userInfokafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_USER_TOPIC))

    //将前2个orderInfo和orderDetailkafka每一行数据转换为样例类对象case class
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfokafkaDStream.map { case (_, value) =>
      //json的value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
      //处理创建日期及小时 2020-02-21 12:12:12
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      //手机号脱敏
      orderInfo.consignee_tel.splitAt(4)._1 + "*******"
      //返回数据
      orderInfo
    }.map(orderInfo => (orderInfo.id, orderInfo))

    //第二个数据流:转化为样例类，然后转化为KV结构
    val order_id_DetailkafkaDStream: DStream[(String, OrderDetail)] = orderDetailkafkaDStream.map { case (_, value) =>
      //json的value转换为样例类
      JSON.parseObject(value, classOf[OrderDetail])
    }.map(orderDetail => (orderDetail.id, orderDetail))

    //数据进行双流join：订单数据join订单详情数据
    //fullOuterJoin:外连接，两个表的数据都包括
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(order_id_DetailkafkaDStream)

    //处理join之后的数据
    val orderInfoAndDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions(iter => {
      //定义集合用于存放JOIN上的数据
      val listBuffer = new ListBuffer[SaleDetail]()

      //获取Redis连接（需要隐式转化）
      val jedisClient: Jedis = RedisUtil.getJedisClient
      implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

      //处理join后的每一条数据
      iter.foreach { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
        //定义info及detail数据的RedisKey
        val orderRedisKey = s"order:$orderId"
        val detailRedisKey = s"detail:$orderId"

        //一：判断当前窗口orderinfo是否为空(isDefined=!isEmpty),非空有数据就取出数据，然后接着计算
        if (orderInfoOpt.isDefined) {
          //取出orderInfoOpt数据
          val orderinfo: OrderInfo = orderInfoOpt.get

          //当orderinfo为非空时，
          // 1.再判断orderDetai是否为有数据,有数据的话就添加至集合
          if (orderDetailOpt.isDefined) {
            val orderDetail: OrderDetail = orderDetailOpt.get
            //组合数据，并添加至集合
            listBuffer += new SaleDetail(orderinfo, orderDetail)

          }
          //2.orderDetail没数据，将orderinfo 转为json，写入redis
          val orderJson: String = Serialization.write(orderinfo)
          jedisClient.set(orderRedisKey, orderJson)
          //设置过期时间，到了这个时间，自动删除
          jedisClient.expire(orderRedisKey, 300)

          //3.查询Redis中的detail缓存，如果存在数据则将redis中的JSON数据放入集合set
          val orderDetailset: util.Set[String] = jedisClient.smembers(detailRedisKey)
          import scala.collection.JavaConversions._
          orderDetailset.foreach(detailJson => {
            //将detailJson转换为OrderDetail样例类对象
            val orderDetail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
            listBuffer += new SaleDetail(orderinfo, orderDetail)
          })
        }
        //二、当前窗口orderInfoOpt没数据
        else {
          //直接获取orderDetail数据(orderDetail肯定有数据)
          val orderDetailInfo: OrderDetail = orderDetailOpt.get

          //查询Redis取出OrderInfo的数据
          if (jedisClient.exists(orderRedisKey)) {
            //Redis中存在OrderInfo,读取数据JOIN之后放入集合
            val orderJson: String = jedisClient.get(orderRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
            listBuffer += new SaleDetail(orderInfo, orderDetailInfo)
          } else {
            //Redis中不存在Orderinfo,将Detail数据写入Redis
            val detailJson: String = Serialization.write(orderDetailInfo)
            jedisClient.sadd(detailRedisKey, detailJson)
            jedisClient.expire(detailRedisKey, 300)
          }
        }
      }
      //关闭连接
      jedisClient.close()
      listBuffer.toIterator

    })
    orderInfoAndDetailDStream.print(100)

    //处理join之后的信息数据:反查Redis，再补上用户信息
    val result: DStream[SaleDetail] = orderInfoAndDetailDStream.mapPartitions(iter => {
      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val detailsResult: Iterator[SaleDetail] = iter.map(saleDetail => {
        //查询Redis中的用户信息
        val userJson: String = jedisClient.get(s"user:${saleDetail.user_id}")
        //将用户信息补全到saleDetail
        val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        //将saleDetail返回
        saleDetail
      })
      jedisClient.close()
      detailsResult
    })
    result.cache()
    result.print()

   /* //写入ES
    result.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        //转换数据结构
        iter.map(saleDetail=>(
          s"${saleDetail.order_id}-${saleDetail.order_detail_id}",saleDetail
        ))
      })

    })*/

    
    ssc.start()
    ssc.awaitTermination()

  }

}
