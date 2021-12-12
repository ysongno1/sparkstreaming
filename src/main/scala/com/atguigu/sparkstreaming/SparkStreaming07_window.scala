package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //3 获取一行数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)


    //4 切割数据
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5 转换数据结构
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //对数据流进行开窗 窗口12s 滑动步长6s
    val wordToOneByWindow: DStream[(String, Int)] = word2oneDStream.window(Seconds(12), Seconds(6))

    //求和
    val resultDStream: DStream[(String, Int)] = wordToOneByWindow.reduceByKey(_ + _)

    resultDStream.print()



    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
