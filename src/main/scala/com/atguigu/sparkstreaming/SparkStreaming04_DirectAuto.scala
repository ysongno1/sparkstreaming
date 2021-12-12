package com.atguigu.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //对接Kafka
    val kafkaDstream: InputDStream[ConsumerRecord[Nothing, Nothing]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(Set("testTopic"), kafkaParams))

    //消费kafka数据进行wordcount
    //1转换数据结构，获取kafka每条记录的value
    val lineDStream: DStream[String] = kafkaDstream.map(record => record.value())

    //2进行wc
    val resultDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //3打印
    resultDStream.print()

    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
