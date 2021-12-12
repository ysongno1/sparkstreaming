package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //通过监控端口创建DStream 读进来的数据为一行行
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //将每一行数据切分 形成一个个单词
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //将单词映射成(word,1)
    val word2OneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //将相同的单词次数做统计
    val result: DStream[(String, Int)] = word2OneDStream.reduceByKey(_ + _)

    result.print()


    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
