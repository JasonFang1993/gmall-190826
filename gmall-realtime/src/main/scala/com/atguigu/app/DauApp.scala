package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
object DauApp {
  def main(args: Array[String]): Unit = {


    //1创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestKafka")


    //2创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3读取Kafka读取Dstream
    val kafkaDString: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP_TOPIC))

    //定义时间格式化 对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4将每一行数据转换为样例类对象
    val startLogDStream: DStream[StartUpLog] = kafkaDString.map { case (_, value) =>
      //5将数据转换为样例类对象
      val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //取出时间戳
      val ts: Long = log.ts

      //将时间戳转换为字符串
      val logDateHour: String = sdf.format(new Date(ts))

      //截取字段
      val logDateHourArr: Array[String] = logDateHour.split(" ")

      //赋值日期&小时
      log.logDate = logDateHourArr(0)
      log.logHour = logDateHourArr(1)

      log
    }


    //5跨批次去重(根据Redis中的数据进行去重)
    val filterByRedis: DStream[StartUpLog] =
      DauHandler.filterDataByRedis(startLogDStream,ssc)

    filterByRedis.cache()

    //6同批次去重
    val filterByBatch: DStream[StartUpLog] =
      DauHandler.filterDataByBatch(filterByRedis)

    filterByBatch.cache()

    //7.将数据保存到Redis 方便下一次的去重使用
    DauHandler.saveMidToRedis(filterByBatch)


    //测试
    filterByBatch.count().print()


    //8有效数据(不做计算)写入HBase
    filterByBatch.foreachRDD(rdd =>
    rdd.saveToPhoenix("GMALL190826_DAU",
      Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

    )
    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
