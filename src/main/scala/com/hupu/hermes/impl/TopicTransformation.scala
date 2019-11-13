package com.hupu.hermes.impl

import com.hupu.hermes.AbstractTopicTransformation

class TopicTransformation(appName: String, topic: String)
    extends AbstractTopicTransformation(appName, topic) {

  /**
    * 获取SQL
    *
    * @return
    */
  override def getSQL(topic: String): String = {
    s"""|(SELECT
        |	t6.input_topic_id,
        |	t6.input_topic,
        |	t6.output_topic_id,
        |	t6.output_topic,
        |	t6.input_field,
        |	t6.input_type,
        |	t6.output_field_ids,
        |	t7.udf
        |FROM
        |	(
        |	SELECT
        |		t3.output_topic_id,
        |		t5.id AS input_topic_id,
        |		t5.topic_name AS input_topic,
        |		t4.topic_name AS output_topic,
        |		t2.field_name AS input_field,
        |		t2.field_type AS input_type,
        |		t1.uid,
        |		t1.output_field_ids
        |	FROM
        |		etl_msg_transform_rules t1
        |		JOIN common_topic_schema t2
        |		JOIN etl_pipeline_config t3
        |		JOIN common_topic_info t4
        |		JOIN common_topic_info t5 ON t1.input_field_ids = t2.id
        |		AND t1.pid = t3.id
        |		AND t4.id = t3.output_topic_id
        |		AND t2.topic_id = t5.id
        |		AND t5.topic_name IN ( "$topic" )
        |	) t6
        |	LEFT JOIN etl_udfs t7 ON t6.uid = t7.id) t_transfer
     """.stripMargin
  }

  override def getTopicSchema(): String = {
    s"""|(SELECT
        |	id,
        |	topic_id,
        |	field_name,
        |	field_type
        |FROM
        |	common_topic_schema ) common_topic_schema
     """.stripMargin
  }

  /**
    * 广播校验规则
    */
  override def bcTopicValidationRule(topic: String): Unit = {}
}
