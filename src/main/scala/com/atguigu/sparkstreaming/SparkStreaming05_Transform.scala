package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    // 在Driver端执行，全局一次
    println("111111111:" + Thread.currentThread().getName)


    lineStream.transform( //原语
      rdd => {
        // 在Driver端执行(ctrl+n JobGenerator)，一个批次一次
        println("222222:" + Thread.currentThread().getName)

        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map(x=>{

          // 在Executor端执行，和单词个数相同
          println("333333:" + Thread.currentThread().getName)

          (x, 1)
        })

        val resultRDD: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        resultRDD
      }
    )


    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
