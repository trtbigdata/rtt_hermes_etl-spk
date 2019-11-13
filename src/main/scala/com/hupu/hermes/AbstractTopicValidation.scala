package com.hupu.hermes

import com.hupu.hermes.service.SparkService
import com.hupu.hermes.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
  * 获取校验规则抽象类
  *
  * @param appName spark application name
  */
abstract class AbstractTopicValidation(appName: String, topic: String)
    extends TopicTrait {
  val sparkSession: SparkSession = SparkUtil.getSparkSession(appName)

  /**
    * 获取校验SQL
    *
    * @return
    */
  def getSQL(topic: String): String

  /**
    * 获取校验规则
    */
  override def bcTopicValidationRule(topic: String) {
    val validationSQL = getSQL(topic)
    SparkService.topicValidationRuleBC(appName, topic, validationSQL)
  }
}
