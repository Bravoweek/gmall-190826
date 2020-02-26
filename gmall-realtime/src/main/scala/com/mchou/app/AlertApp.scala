package com.mchou.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mchou.bean.EventInfo
import com.mchou.constants.GmallConstants
import com.mchou.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.control.Breaks._
import scala.collection.mutable

object AlertApp {
  def main(args: Array[String]): Unit = {
    //一、常规操作，转换成样例类的sparkstreaming：
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    //读取kafka数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_EVENT_TOPIC))

    val sdf = new SimpleDateFormat("yyy-mm-dd HH")

    //将数据转换为样例类对象
    val eventLogDStream: DStream[EventInfo] = kafkaDStream.map { case (_, value) =>
      val log: EventInfo = JSON.parseObject(value, classOf[EventInfo])

      //添加日期及小时
      val ts: Long = log.ts
      val datestr: String = sdf.format(new Date(ts))
      log.logDate = datestr.split(" ")(0)
      log.logHour = datestr.split(" ")(1)

      log
    }

    //二、预警逻辑
    //根据条件产生预警日志：同一设备，30秒内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。
    //1.开窗30秒 -> 30秒内
    val windoweventLogDStream: DStream[EventInfo] = eventLogDStream.window(Seconds(30))

    //2.分组出来同一设备：按照mid进行分组
    val MidToLogIter: DStream[(String, Iterable[EventInfo])] = windoweventLogDStream.map(log=>(log.uid,log)).groupByKey()

    //先选择需要预警的mid:即是否使用三个id && 是否有点击行为
    //
    MidToLogIter.map{case (mid,logIter)=>

      //创建集合用于存放领取优惠券所对应的uid
      val uid = new mutable.HashSet[String]()
      //领取优惠券的id对应的商品id
      val itemIds = new mutable.HashSet[String]()
      //用户行为
      val event = new mutable.HashSet[String]()
      //是否有浏览商品行为的标志位:默认为没有
      var noClick:Boolean=true

      //遍历数据集
      logIter.foreach(log=>{

        //添加用户行为,将用户的行为放到集合
        event.add(log.evid)
        //判断是否存在浏览商品行为,存在的话就判断为false，结束进程
        if ("clickItem".equals(log.evid)){
          noClick=false
          //判断是否为领券行为，是的话就将uid和对应商品添加到集合里
        }else if ("coupon".equals(log.evid)){
          uid.add(log.uid)
          itemIds.add(log.itemid)
        }
      })

    }















    ssc.start()
    ssc.awaitTermination()
  }
}
