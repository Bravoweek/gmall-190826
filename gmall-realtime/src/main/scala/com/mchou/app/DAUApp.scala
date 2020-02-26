package com.mchou.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mchou.bean.StartUpLog
import com.mchou.constants.GmallConstants
import com.mchou.handler.DauHandler
import com.mchou.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._


object DAUApp {
  def main(args: Array[String]): Unit = {
    //1.创建参数
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DAUApp")

    //2.创建StreamingContext流式数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取kafka的数据创建DStream
    val kafkaDstream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP_TOPIC))

    //定时时间格式
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将读取kafka的json类型数据转换为样例类,补充两个时间字段
    val startLogDStream: DStream[StartUpLog] = kafkaDstream.map { case (_, value) =>
      //解析jison的value转化为样例类
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //取出时间戳，转换成年月日小时样式
      val ts: Long = startUpLog.ts
      val logDateHour: String = sdf.format(new Date(ts))
      //将日期小时分割开
      val logDateHourArr: Array[String] = logDateHour.split(" ")
      //把解析出来的数据赋值到样例类的字段
      startUpLog.logDate = logDateHourArr(0)
      startUpLog.logHour = logDateHourArr(1)
      startUpLog
    }

    //对数据流进行过滤操作：5.跨批次去重（根据redis中数据去重）
    val filterByRedis: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLogDStream)
    filterByRedis.cache()

    //6.同批次去重
    val filterByBatch: DStream[StartUpLog] = DauHandler.filterDataByBach(filterByRedis)

    filterByBatch.cache()

    //7,将数据保存至Redis，以供下一次去重使用
    DauHandler.saveMidRedis(filterByBatch)

    //8.将所有数据（没计算的）写入HBase
    filterByBatch.foreachRDD(rdd=>
    rdd.saveToPhoenix("GMALL19826_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
      new Configuration,Some("hadoop106,hadoop107,hadoop108:2181"))
    )

    //测试
//   filterByBatch.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
