package com.hupu.hermes

import com.hupu.hermes.service.SparkService
import com.hupu.hermes.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
  * 获取转换规则抽象类
  *
  * @param appName spark application name
  */
abstract class AbstractTopicTransformation(appName: String, topic: String)
    extends TopicTrait {
  val sparkSession: SparkSession = SparkUtil.getSparkSession(appName)

  /**
    * 获取转换SQL
    *
    * @return
    */
  def getSQL(topic: String): String

  /**
    *
    * @return
    */
  def getTopicSchema: String

  /**
    * 获取校验规则
    */
  override def bcTopicTransferRule(topic: String) {
    val transformationSQL = getSQL(topic)
    val topicSchema = getTopicSchema
    SparkService.topicTransformationRuleBC(appName,
                                           topic,
                                           transformationSQL,
                                           topicSchema)
  }
}
