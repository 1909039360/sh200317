package com.atguigu.app

import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.common.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._  //注意这里使用隐式转换的方式

/**
 * Author: doubleZ
 * Datetime:2020/8/17   0:00
 * Description:
 */
object Dau_01 {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //2 创建StreamingConText
    val ssc: StreamingContext = new  StreamingContext(sparkConf,Seconds(5))
    //3 读取Kafka Start主题的数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    //4 将读取的数据转换为样例类对象（logDate和logHour）
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a. 取出Value
      val value: String = record.value()
      //b.转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //c.取出时间戳字段解析给logDate和logHour赋值
      val ts: Long = startUpLog.ts
      val dateHour: String = simpleDateFormat.format(new Date(ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //返回值
      startUpLog
    })
    startUpLogDStream.cache()
    startUpLogDStream.count().print()
    // 5.根据Redis中保存的数据进行跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)
    // 6.对第一次去重后的数据做同批次去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
    // 7.将两次去重后的数据(mid)写入到Redis
    DauHandler.saveMidToRedis(filterByGroupDStream)
    //8. 将数据保存至HBase(Phoenix)
    filterByGroupDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL2020_DAU",//表明
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()), //列
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    //9. 启动任务
    ssc.start()
    ssc.awaitTermination()
  }


}
