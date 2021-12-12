package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

    //创建RDD对列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    //利用RDD队列创建DStream
    // oneAtATime = true 默认，一次读取队列里面的一个数据
    // oneAtATime = false， 按照设定的批次时间，读取队列里面数据
    val rddDStream: InputDStream[Int] = ssc.queueStream(rddQueue)

    val resultDStream: DStream[Int] = rddDStream.reduce(_ + _)

    resultDStream.print()

    //4.启动ssc
    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}
