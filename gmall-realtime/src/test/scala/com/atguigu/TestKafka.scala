package com.atguigu

import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafka {

    def main(args: Array[String]): Unit = {

      //1.创建SparkConf
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestKafka")

      //2.创建StreamingContext
      val ssc = new StreamingContext(sparkConf,Seconds(5))

      //3.读取kafka数据创建DString
      val kafkaDString: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_STARTUP_TOPIC))

      //4.取出value并且打印
kafkaDString.map(_._2).print()

      //启动任务
      ssc.start()
      ssc.awaitTermination()

    }

}
