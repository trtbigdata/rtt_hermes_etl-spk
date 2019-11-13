package com.hupu.hermes.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author yanjiajun
  */
object SparkUtil extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(SparkUtil.getClass)
  @transient var instance: SparkSession = _

  val dbMap: Map[String, String] = Map(
    "driver" -> "com.mysql.jdbc.Driver",
    "url" -> ConfigUtil.getProperty("dimension.url"),
    "user" -> ConfigUtil.getProperty("username"),
    "password" -> ConfigUtil.getProperty("password")
  )

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> ConfigUtil.getProperty("inKafka.bootstrap.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ConfigUtil.getProperty("inKafka.group.id"),
    "auto.offset.reset" -> ConfigUtil.getProperty("inKafka.auto.offset.reset"),
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * 根据环境获取或者创建sparkSession
    *
    * @param sparkConf SparkConf
    * @return
    */
  def getSparkSession(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder.config(sparkConf).getOrCreate()
    }
    instance
  }

  /**
    * 根据环境获取或者创建sparkSession
    *
    * @param appName applicationName
    * @return
    */
  def getSparkSession(appName: String): SparkSession =
    SparkSession.builder.appName(appName).getOrCreate()

  /**
    *
    * @param ssc StreamingContext
    * @param inKafkaTopics inputKafkaTopic
    * @return
    */
  def createDirectStream(
    ssc: StreamingContext,
    inKafkaTopics: Set[String]
  ): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      // spark2.x的分配任务方式之一，表示致性的方式分配分区所有 executor 上（主要是为了分布均匀）
      LocationStrategies.PreferConsistent,
      // 允许你订阅固定的 topic 集合
      ConsumerStrategies.Subscribe[String, String](inKafkaTopics, kafkaParams)
    )
  }

}
