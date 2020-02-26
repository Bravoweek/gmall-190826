package com.mchou.app

import com.alibaba.fastjson.JSON
import com.mchou.bean.OrderInfo
import com.mchou.constants.GmallConstants
import com.mchou.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object GMVApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GMVApp")

    val ssc = new StreamingContext(conf,Seconds(3))

    //读取kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_ORDER_TOPIC))

    //将kafka每一行数据转换为样例类对象case class ：手机号脱敏，生成数据日期
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map { case (_, value) => {
      //json的value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      //添加日期及小时
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      //将日期和小时添加进样例类
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      //手机脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"

      orderInfo
    }
    }
    //数据存到HBASE
    orderInfoDStream.foreachRDD(rdd=>{
      println(rdd.collect().mkString("\n"))
      rdd.saveToPhoenix("GMALL2019_ORDER_INFO",Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),Some("hadoop106,hadoop107,hadoop108:2181"))
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
