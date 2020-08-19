package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.common.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import scala.util.parsing.json.JSONObject

/**
 * Author: doubleZ
 * Datetime:2020/8/18   20:29
 * Description:
 */
object GmvApp1 {
  def main(args: Array[String]): Unit = {
//    1.创建SparkConf 以及StreamingContext
      val sparkConf: SparkConf = new SparkConf().setMaster("GMVApp1").setMaster("local[*]")
      val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
//    2.读取Kafka order order_info主题数据创建流
      val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER,ssc)
//    3.将每一行数据转换为样例类:给日期及小时字段重新赋值,给联系人手机号脱敏.
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //a.转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.给日期及小时段重新赋值
      val create_time: String = orderInfo.create_time
      val timeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)
      //c.给联系人收集号脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val telTuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "********"
      //d.返回数据
      orderInfo
    })
    orderInfoDStream.cache()
    orderInfoDStream.print()
//    4.将数据写入Phoenix
    //这里注意foreachRDD 没有返回值 和transform情况不一样 transform是有返回值的
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("gmall200317_order_info",
        classOf[OrderInfo].getDeclaredFields.map(_.getName),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })
//    5.启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
