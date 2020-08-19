package com.atguigu.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.common.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Author: doubleZ
 * Datetime:2020/8/17   21:16
 * Description:
 */
object Dau_02 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_01")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    //此时消息是k v 类型的键值对 ma排,将 v取出来 v是json类型的支付串 需要做转换
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val value: String = record.value()
      // 通过 json直接转换为样例类
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      val dateHour: String = simpleDateFormat.format(ts)
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    })
    // 上面的代码是将 消息map(k,v) 其中 v是 json类的消息 通过 JSON.pareObject(text,classOf[样例类])进行转换
    // 最后转换为 DStream[样例类中的形式]
    //startUpLogDStream
    //5 根据Reids中保存的数据进行跨批次去重 跨批次去重 主要思路  将不重复的mid写入到 redis set 如果发现redis已经存在 则说明这条数据重复
    DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    //6 对第一次去重后的数据做同批次去重
    //7 将两次去重后的数据(mid)写入Redis(先写这一步)
    DauHandler.saveMidToRedis(startUpLogDStream)
    //8 将数据保存至HBase(Phoenix)

  }

}
