package com.hupu.hermes.impl

import com.hupu.hermes.AbstractTopicValidation

class TopicValidation(appName: String, topic: String)
    extends AbstractTopicValidation(appName, topic) {

  /**
    * 获取SQL
    *
    * @return
    */
  override def getSQL(topic: String): String = {
    s"""|SELECT
        |	t3.topic_name,
        |	t1.filter_sql
        |FROM
        |	etl_msg_validation_rules t1
        |	JOIN etl_pipeline_config t2
        |	JOIN common_topic_info t3
        | ON t1.pid = t2.id
        |	AND t2.input_topic_id = t3.id
        |	AND t3.topic_name="$topic"
     """.stripMargin
  }

  /**
    * 广播转换规则
    */
  override def bcTopicTransferRule(topic: String): Unit = {}
}
