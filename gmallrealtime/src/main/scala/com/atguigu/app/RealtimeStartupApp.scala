package com.atguigu.app

/**
 * Author: doubleZ
 * Datetime:2020/8/15   21:29
 * Description:
 */
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.common.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._
object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2020")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //       startupStream.map(_.value()).foreachRDD{ rdd=>
    //         println(rdd.collect().mkString("\n"))
    //       }

    val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map { log =>
      // println(s"log = ${log}")
      val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      startUpLog
    }
  }
}