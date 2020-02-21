package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable

object DauHandler {
  /**
    * 同批次去重
    *
    * @param filterByRedis 根据Redis中的数据集过滤后的结果
    */
  def filterDataByBatch(filterByRedis: DStream[StartUpLog]) = {

    //a将数据转化为((date,mid),log)
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] =
      filterByRedis.map(log =>

        ((log.logDate, log.mid), log))

    //b按照key分组
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //c.组内按照日期排序,取第一个元素
    //    val value1: DStream[List[StartUpLog]] =
    // dateMidToLogIterDStream.map { case ((_, _), logIter) =>
    //
    //      val logs: List[StartUpLog] = logIter.toList.sortWith(_.ts < _.ts).take(1)
    //      logs
    //    }
    //    value1

    //flatmap 结果集直接压平操作
    val value: DStream[StartUpLog] =
      dateMidToLogIterDStream.flatMap {
        case ((_, _), logIter) =>

          val logs: List[StartUpLog] =
            logIter
              .toList
              .sortWith(_.ts < _.ts).take(1)
          logs

      }

    //返回数据
    value


  }

  /**
    * 跨批次去重(根据Redis中的数据去重)
    *
    * @param startLogDStream 从kafka读取原始数据
    * @return
    */
  def filterDataByRedis(startLogDStream: DStream[StartUpLog],ssc :StreamingContext): DStream[StartUpLog] = {
    startLogDStream.transform(rdd => {

//      //查询Redis数据 使用广播变量发送到Executor
//      //获取连接
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//
//      val ts: Long = System.currentTimeMillis()
//      //date -> 今天/昨天
//      var now: Date = new Date()
//      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//      var today = dateFormat.format(now)
//      val todaydate = s"dau:${today}"
//
//      val cal: Calendar = Calendar.getInstance()
//      cal.add(Calendar.DATE, -1)
//      val yesterday: String = dateFormat.format(cal.getTime)
//      val yesterdaydate = s"dau:${yesterday}"
//
//      val yes1: util.Set[String] = jedisClient.smembers(yesterdaydate)
//      val today1: util.Set[String] = jedisClient.smembers(todaydate)
//
//      val map: Map[String, util.Set[String]] = Map[String, util.Set[String]]("yes" -> yes1, "today" -> today1)
//      import scala.collection.JavaConversions._
//      //mutable.Map("yes" -> strings,"today" -> strings1)
//
//      //广播变量
//      val dateYesBC: Broadcast[Map[String, util.Set[String]]] = ssc.sparkContext.broadcast(map)
//
//      //释放连接
//     jedisClient.close


      rdd.mapPartitions(iter => {
//        //方法二 获取连接
//        // val jedisClient: Jedis = RedisUtil.getJedisClient
//        val stringToLogs: Map[String, List[StartUpLog]] = iter.toList.groupBy(_.logDate)
//        dateYesBC.value.take(0).contains(stringToLogs.get(0))
//
//
//        //过滤
//        val logs: Iterator[StartUpLog] =
//
//          iter.filter(log => {
//
//            val logDate = s"dau:${log.logDate}"
//
//
//
//            true
//          })


        //方法一 获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //过滤
        val logs: Iterator[StartUpLog] =

          iter.filter(log => {

            val redisKey = s"dau:${log.logDate}"
            !jedisClient.sismember(redisKey, log.mid)

          })

        //释放连接
        jedisClient.close()
        logs
      })
    })
  }

  /**
    * 将2次过滤后的数据集中的mid存到Redis里面
    *
    * @param filterByRedis
    * @return
    */
  def saveMidToRedis(filterByRedis: DStream[StartUpLog]) = {

    filterByRedis.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //将数据写入到Redis
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //释放连接
        jedisClient.close()

      })
    })
  }

}
