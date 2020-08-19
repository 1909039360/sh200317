package com.atguigu.handler

import java.time.LocalDate
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.BROADCAST
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * Author: doubleZ
 * Datetime:2020/8/17   0:00
 * Description:
 */
object DauHandler {
  // 6.对第一次去重后的数据做同批次去重
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //a.转换结构
    val midDateToStartLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })
    //b.按照Key分组
    val midDateToStartLogDStreamIter: DStream[(String, Iterable[StartUpLog])] = midDateToStartLogDStream.groupByKey()
    //c.组内取时间戳最小的一条数据
    val midDateToStartLogDStreamList: DStream[(String, List[StartUpLog])] = midDateToStartLogDStreamIter.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //d.压平
    val midDateToStartLogList: DStream[StartUpLog] = midDateToStartLogDStreamList.flatMap(_._2)
    midDateToStartLogList
  }

  // 根据Redis中保存的数据进行跨批次去重
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], ssc: SparkContext):DStream[StartUpLog] = {
    //方案一:单条过滤数据
    startUpLogDStream.filter(startLog => {
      //a.获取Redis 连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b. 查询Redis中是否存在该mid
      val exist: Boolean = jedisClient.sismember(s"dau:${startLog.logDate}",startLog.mid)
      //c.归还连接
      jedisClient.close()
      //d.返回值
      !exist
    })
    //    value1
    //方案二:使用分区操作代替单条数据操作,减少连接数
    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
      //a. 获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.过滤数据
      val filterIter: Iterator[StartUpLog] = iter.filter(StartUpLog => {
        !jedisClient.sismember(s"dau:${StartUpLog.logDate}", StartUpLog.mid)
      })
      //c.归还连接
      jedisClient.close()
      //d.返回值
      filterIter
    })
    //    value2
    //方案三:每个批次获取一次Redis中的Set集合数据,广播值Executor
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //a.获取Redis中Set集合数据并广播,每个批次在Driver端执行一次
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val today: String = LocalDate.now().toString
      val midSet: util.Set[String] = jedisClient.smembers(s"dau:$today")
      val midSetBc: Broadcast[util.Set[String]] = ssc.broadcast(midSet)
      //b.在Executor端使用广播变量进行去重
      rdd.filter(startLog => {
        !midSetBc.value.contains(startLog.mid)
      })
    })
    value3
  }

  //将两次去重后的数据(mid)写入Redis
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {
    val value1: Unit = startUpLogDStream.foreachRDD(rdd => {
      //使用foreachPartition代替foreach,减少链接数的获取与释放
      rdd.foreachPartition(iter => {
        //a. 获取链接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b 数据操着
        iter.foreach(StartUpLog => {
          val redisKey: String = s"dau:${StartUpLog.logDate}"
          jedisClient.sadd(redisKey, StartUpLog.mid)
        })
        //c. 归还链接
        jedisClient.close()
      })

    })

  }
  //注意下面这种写法不可以
  def saveMidToRedis1(startUpLogDStream: DStream[StartUpLog]): Unit = {
    val jedisClient: Jedis = RedisUtil.getJedisClient //这种写法不可以 因为 考虑到序列化

    var key = s"dau${LocalDate.now().toString}" //这里是可以的 因为String是序列化过的
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreach(startlog =>{
        jedisClient.sadd(key,startlog.mid)
      })
    })
    jedisClient.close()

  }
  def saveMidToRedis2(startUpLogDStream: DStream[StartUpLog]): Unit = {

    startUpLogDStream.foreachRDD(rdd =>{
      rdd.foreachPartition( iter =>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(startLog =>{
          var redisKey = s"dau${startLog.logDate}"
          jedisClient.sadd(redisKey,startLog.mid)
        })
        jedisClient.close()
      })
    })

  }

  def filterByRedis1(startUpLogDStream: DStream[StartUpLog], ssc: SparkContext):DStream[StartUpLog] = {
    // 去重 获取连接 读取 redis中的数据 第一种写法 每次获取一次redis连接
    val value1: DStream[StartUpLog] = startUpLogDStream.filter(startLog => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val exist: Boolean = jedisClient.sismember(s"dau${startLog.logDate}", startLog.mid)

      jedisClient.close()
      !exist
    })
    //上面的这种方法 每条数据建立一次连接 频繁建立连接 消耗较大
    //方案二 使用 mapPartitions 代替 是的一个分区建立一次
    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val logs: Iterator[StartUpLog] = iter.filter(startLog => {
        !jedisClient.sismember(s"dau${startLog.logDate}", startLog.mid)
      })

      jedisClient.close()
      logs
    })

    //方案三:每个批次获取一次Redis中的Set集合数据,广播值Executor
    //方案三:每批次获取一次Redis中的Set集合数据,广播值Executor

    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      // 1 获取需要广播的值
      val jedisClient: Jedis = RedisUtil.getJedisClient
      var redisKey = s"dau${LocalDate.now().toString}"
      val midSet: util.Set[String] = jedisClient.smembers(redisKey)
      val midSetBc: Broadcast[util.Set[String]] = ssc.broadcast[util.Set[String]](midSet)
      //b.在Executor端 使用广播变量进行去重
      rdd.filter(startLog => {
        !midSetBc.value.contains(startLog.mid)
      })
    })
    val value4: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val midKeySet: util.Set[String] = jedisClient.smembers(s"dau${LocalDate.now().toString}")
      val midSetBc: Broadcast[util.Set[String]] = ssc.broadcast[util.Set[String]](midKeySet)
      //b.在Executor端 使用广播变量进行去重
      rdd.filter(startLog => {
        !midSetBc.value.contains(startLog.mid)
      })

    })
    //方案3 每次获取一次Redis连接 获取Set集合的数据 使用广播变量
    startUpLogDStream.transform(rdd =>{
      //获取Redis表中的数据
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val midSet: util.Set[String] = jedisClient.smembers(s"dau${LocalDate.now().toString}")
      val midSetBc: Broadcast[util.Set[String]] = ssc.broadcast[util.Set[String]](midSet)
      rdd.filter(startLog =>{
        ! midSetBc.value.contains(startLog.mid)
      })
    })
    val value5: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val midSet: util.Set[String] = jedisClient.smembers(s"dat${LocalDate.now().toString}")
      val midSetBc: Broadcast[util.Set[String]] = ssc.broadcast(midSet)
      rdd.filter(startLog => {
        !midSetBc.value.contains(startLog.mid)
      })
    })


    return  value3;

  }
}


