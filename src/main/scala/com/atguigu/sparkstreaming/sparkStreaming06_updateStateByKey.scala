package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming06_updateStateByKey {


  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.checkpoint("ck")

    //3 获取一行数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)


    //4 切割数据
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5 转换数据结构
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //6 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val resultDstream: DStream[(String, Int)] = word2oneDStream.updateStateByKey(updateFunc)

    resultDstream.print()

    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc =(current:Seq[Int], state:Option[Int]) => {
    //1.先对当前批次数据求和
    val currentSum: Int = current.sum

    //2.取出历史数据的和
    val stateSum: Int = state.getOrElse(0)

    //3.将当前批次的和加上历史状态的和，返回
    Some(currentSum + stateSum)
  }

}
