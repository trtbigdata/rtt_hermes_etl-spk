package com.hupu.hermes.service

import java.time.LocalDateTime
import java.util.Properties

import com.hupu.hermes.handlers.TopicFlowResHandler
import com.hupu.hermes.launch.TopicStreamingLaunch.dateFormat
import com.hupu.hermes.model.EtlFlowMonitor
import com.hupu.hermes.utils.{
  ConfigUtil,
  Constants,
  DBUtil,
  GroupConcat,
  KafkaSink,
  SparkUtil,
  UDFs
}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * @author yanjiajun
  */
object SparkService extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(SparkService.getClass)
  @volatile var topicValidation: Broadcast[Map[String, String]] = _
  @volatile var inputOutputTopic: Broadcast[Map[String, String]] = _
  @volatile var transferFieldSQL: Broadcast[Map[String, String]] = _
  @volatile var outputFields: Broadcast[Map[String, String]] = _

  /**
    *
    * @param sparkSession SparkSession
    * @param topic topic
    * @return
    */
  def initAccumulator(sparkSession: SparkSession,
                      topic: String): Map[String, LongAccumulator] = {
    val currentTime: String = LocalDateTime.now.format(dateFormat)
    val today: String = currentTime.substring(0, 8)
    val sql =
      s"""|SELECT
          |	dt,
          |	topic,
          |	num_from_kafka,
          | num_into_kafka
          |FROM
          |	etl_flow_monitor WHERE dt = "$today" AND topic = "$topic"
       """.stripMargin
    // 将查询出来的结果封装到EtlFlowMonitor中
    val etlFlow: EtlFlowMonitor =
      DBUtil.query(sql, new TopicFlowResHandler).asInstanceOf[EtlFlowMonitor]
    // fromKafka的累加器
    val recordsFromKafka =
      sparkSession.sparkContext.longAccumulator(Constants.ACCU_FROM_KAFKA)
    // intoKafka的累计器
    val recordsIntoKafka =
      sparkSession.sparkContext.longAccumulator(Constants.ACCU_INTO_KAFKA)

    val accumulatorMap = mutable.LinkedHashMap[String, LongAccumulator]()
    if (etlFlow != null) {
      recordsFromKafka.add(etlFlow.getNumFromKafka)
      recordsIntoKafka.add(etlFlow.getNumIntoKafka)
    }
    accumulatorMap += (Constants.ACCU_FROM_KAFKA -> recordsFromKafka)
    accumulatorMap += (Constants.ACCU_INTO_KAFKA -> recordsIntoKafka)
    accumulatorMap.toMap
  }

  /**
    * 广播ip地址解析库
    *
    * @param sparkSession SparkSession
    */
  def ipDbBC(
    sparkSession: SparkSession
  ): Broadcast[Array[(Long, Long, String, String, String)]] = {
    val ipRDD =
      sparkSession.sparkContext.textFile(ConfigUtil.getProperty("ipDB.path"))
    val ipResourceRdd = ipRDD.map(a => {
      val splitArr = a.split(",")
      (
        splitArr(0).toLong,
        splitArr(1).toLong,
        splitArr(2),
        splitArr(3),
        splitArr(4)
      )
    })
    val ipResourceData = ipResourceRdd.collect()
    val ipDB = sparkSession.sparkContext.broadcast(ipResourceData)
    ipDB
  }

  /**
    * 广播kafkaProducer
    * @param sparkSession
    * @return
    */
  def kafkaProducer(
    sparkSession: SparkSession
  ): Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", Constants.OUT_KAFKA_SERVERS)
      p.setProperty(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      p.setProperty(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      p.setProperty("compression.type", "snappy")
      p.setProperty("batch.size", "65536")
      p
    }
    sparkSession.sparkContext.broadcast(
      KafkaSink[String, String](kafkaProducerConfig)
    )
  }

  /**
    *
    * @param sparkSession SparkSession
    */
  def registerUDFs(sparkSession: SparkSession): Unit = {
    sparkSession.udf
      .register(Constants.PARSE_PATH, (str: String) => UDFs.parsePath(str))
    sparkSession.udf
      .register(Constants.PARSE_TIME, (str: String) => UDFs.parseTimeStamp(str))
    sparkSession.udf.register(
      Constants.PARSE_GENDER,
      (gender: Int) => UDFs.parseGender(gender)
    )
    sparkSession.udf.register(
      Constants.PARSE_AB_TEST,
      (abString: String) => UDFs.parseABTest(abString)
    )
    val ipDB = ipDbBC(sparkSession)
    UDFs.registerParseRegion(sparkSession, ipDB)
  }

  /**
    * 广播topic校验规则
    *
    * @param appName       spark application name
    * @param topic         kafka topic
    * @param validationSQL 校验规则
    */
  def topicValidationRuleBC(appName: String,
                            topic: String,
                            validationSQL: String): Unit = {
    val sparkSession = SparkUtil.getSparkSession(appName)
    val validateMap = mutable.LinkedHashMap[String, String]()
    val conn = DBUtil.getConnection
    val ps = conn.prepareStatement(validationSQL)
    val rs = ps.executeQuery()
    while (rs.next()) {
      val topic_name = rs.getString(1)
      val filter_sql = rs.getString(2)
      validateMap += (topic_name -> filter_sql)
    }
    DBUtil.closeConnection()
    topicValidation = sparkSession.sparkContext.broadcast(validateMap.toMap)
  }

  /**
    * 广播topic转换规则
    *
    * @param appName       spark application name
    * @param topic         kafka topic
    * @param validationSQL 校验SQL
    */
  def topicTransformationRuleBC(appName: String,
                                topic: String,
                                validationSQL: String,
                                topicSchema: String): Unit = {
    val sparkSession = SparkUtil.getSparkSession(appName)
    val transferDbMap = SparkUtil.dbMap.+("dbtable" -> validationSQL)
    var transferDF = sparkSession.read
      .format("jdbc")
      .options(transferDbMap)
      .load() //拆解","分隔的输出字段id
    import org.apache.spark.sql.functions._
    transferDF = transferDF
      .select(
        col(Constants.INPUT_TOPIC_ID),
        col(Constants.OUTPUT_TOPIC_ID),
        col(Constants.INPUT_TOPIC),
        col(Constants.OUTPUT_TOPIC),
        col(Constants.INPUT_FIELD),
        col(Constants.INPUT_TYPE),
        col(Constants.UDF),
        col(Constants.OUTPUT_FIELD_IDS)
      )
      .withColumn(
        Constants.OUTPUT_FIELD_IDS,
        explode(split(col(Constants.OUTPUT_FIELD_IDS), ","))
      )
    val fieldDbMap = SparkUtil.dbMap.+("dbtable" -> topicSchema)
    val fieldsDF = sparkSession.read.format("jdbc").options(fieldDbMap).load()
    //关联字段信息表获取被解析的输出字段名
    val parsedTransferDF = fieldsDF
      .join(
        transferDF,
        transferDF(Constants.OUTPUT_TOPIC_ID) === fieldsDF(Constants.TOPIC_ID)
          && transferDF(Constants.OUTPUT_FIELD_IDS) === fieldsDF(Constants.ID)
      )
      .withColumnRenamed(Constants.FIELD_NAME, Constants.OUTPUT_FIELD)
      .withColumnRenamed(Constants.FIELD_TYPE, Constants.OUTPUT_TYPE)
      .drop(Constants.TOPIC_ID, Constants.ID)
    parsedTransferDF.createOrReplaceTempView(Constants.TBL_FIELD_DETAIL)
    //将需要转换的字段用","连接
    val outputFieldDF = parsedTransferDF
      .groupBy(
        col(Constants.INPUT_TOPIC),
        col(Constants.OUTPUT_TOPIC),
        col(Constants.INPUT_FIELD),
        col(Constants.INPUT_TYPE),
        col(Constants.UDF)
      )
      .agg(GroupConcat(col(Constants.OUTPUT_FIELD)))
      .withColumnRenamed("groupconcat$(output_field)", Constants.OUTPUT_FIELD)
    //按UDF groupBy 统计个数
    val oneInMultiOutDF = parsedTransferDF
      .where(Constants.UDF_IS_NOT_NULL)
      .groupBy(Constants.INPUT_TOPIC, Constants.UDF)
      .agg(count("*"))
      .withColumnRenamed(Constants.INPUT_TOPIC, Constants._TOPIC)
      .withColumnRenamed(Constants.UDF, Constants._UDF)

    /**
      * 左连接，若udf个数>1,则拼接UDF解析,将UDF名称作为解析后的输出字段名;
      * udf=1,拼接UDF解析,将MySQL中配置的输出字段作为输出字段名
      * 其他字段按MySQL中配置的输出字段作为输出字段名
      * 最终拼接成解析SQL
      */
    val parseSqlDF = outputFieldDF
      .join(
        oneInMultiOutDF,
        outputFieldDF(Constants.INPUT_TOPIC) === oneInMultiOutDF(
          Constants._TOPIC
        )
          && outputFieldDF(Constants.UDF) === oneInMultiOutDF(Constants._UDF),
        "left"
      )
      .withColumn(
        Constants.PARSE_FIELD,
        when(
          col(Constants._UDF).isNotNull && col(Constants.COUNT_1) > 1,
          concat_ws(
            " ",
            col(Constants.UDF),
            lit("("),
            col(Constants.INPUT_FIELD),
            lit(") AS "),
            col(Constants.UDF)
          )
        ).when(
            col(Constants._UDF).isNotNull && col(Constants.COUNT_1).equalTo(1),
            concat_ws(
              Constants.SPACE,
              col(Constants.UDF),
              lit("("),
              col(Constants.INPUT_FIELD),
              lit(") AS "),
              col(Constants.OUTPUT_FIELD)
            )
          )
          .otherwise(col(Constants.OUTPUT_FIELD))
      )
      .groupBy(col(Constants.INPUT_TOPIC), col(Constants.OUTPUT_TOPIC))
      .agg(
        GroupConcat(col(Constants.PARSE_FIELD)),
        GroupConcat(col(Constants.INPUT_FIELD))
      )
      .withColumnRenamed("groupconcat$(parse_field)", Constants.FIELD_CONCAT)
      .withColumnRenamed("groupconcat$(input_field)", Constants.INPUT_FIELDS)
    //广播input_topic->output_topic
    import sparkSession.implicits._
    val inOutTopicMap = parsedTransferDF
      .select(Constants.INPUT_TOPIC, Constants.OUTPUT_TOPIC)
      .dropDuplicates()
      .as[(String, String)]
      .collect
      .toMap
    inputOutputTopic = sparkSession.sparkContext.broadcast(inOutTopicMap) //转换后字段拼接并广播
    concatOutputFieldBC(sparkSession, topic)
    //转换SQL广播
    val parseFieldMap = parseSqlDF
      .select(Constants.INPUT_TOPIC, Constants.FIELD_CONCAT)
      .as[(String, String)]
      .collect
      .toMap
    transferFieldSQL = sparkSession.sparkContext.broadcast(parseFieldMap)
  }

  /**
    * 拼接转换后的字段
    *
    * @param sparkSession SparkSession
    * @param topic        kafka topic
    * @return
    */
  def concatOutputFieldBC(sparkSession: SparkSession, topic: String): Unit = {
    //拼接UDF转换后的字段
    val udfFieldSql =
      s"""|SELECT
          |	t1.input_topic,
          |	t2.output_field,
          |	t1.udf
          |FROM
          |	( SELECT input_topic, udf FROM tbl_field_detail WHERE input_topic = "$topic" GROUP BY input_topic, udf HAVING
          | count( 1 ) > 1 ) t1
          |	JOIN tbl_field_detail t2 ON t1.input_topic = t2.input_topic
          |	AND t1.udf = t2.udf
       """.stripMargin
    val parseFieldDF = sparkSession
      .sql(udfFieldSql)
      .withColumn(
        Constants.GET_FROM_MAP,
        concat_ws(
          Constants.SPACE,
          concat(
            col(Constants.UDF),
            lit("['"),
            col(Constants.OUTPUT_FIELD),
            lit("'] AS")
          ),
          col(Constants.OUTPUT_FIELD)
        )
      )
      .groupBy(Constants.INPUT_TOPIC)
      .agg(GroupConcat(col(Constants.GET_FROM_MAP)))
      .withColumnRenamed("groupconcat$(get_from_map)", Constants.OUTPUT_FIELD)
    //拼接无需转换的字段
    val noUdfFieldSql =
      s"""|SELECT
          |	input_topic,
          |	output_field
          |FROM
          |	tbl_field_detail
          |WHERE
          |input_topic = "$topic" AND udf IS NULL
          |UNION ALL
          |SELECT
          |	t1.input_topic,
          |	t1.output_field
          |FROM
          |	tbl_field_detail t1
          |	JOIN ( SELECT input_topic, udf FROM tbl_field_detail WHERE input_topic = "$topic" GROUP BY input_topic, udf
          | HAVING count( 1 ) = 1 ) t2
          | ON t1.input_topic = t2.input_topic
          |	AND t1.udf = t2.udf
       """.stripMargin
    val noParseFieldDF = sparkSession
      .sql(noUdfFieldSql)
      .groupBy(Constants.INPUT_TOPIC)
      .agg(GroupConcat(col(Constants.OUTPUT_FIELD)))
      .withColumnRenamed("groupconcat$(output_field)", Constants.OUTPUT_FIELD)
    //UNION ALL两部分输出字段
    val allFieldsDF = parseFieldDF
      .union(noParseFieldDF)
      .groupBy(Constants.INPUT_TOPIC)
      .agg(GroupConcat(col(Constants.OUTPUT_FIELD)))
      .withColumnRenamed("groupconcat$(output_field)", Constants.ALL_FIELDS)
    import sparkSession.implicits._
    val outputFieldsMap = allFieldsDF
      .select(Constants.INPUT_TOPIC, Constants.ALL_FIELDS)
      .as[(String, String)]
      .collect
      .toMap
    outputFields = sparkSession.sparkContext.broadcast(outputFieldsMap)
  }

  /**
    *
    * @param topicName    spark topic
    * @param currentTime  当前时间yyyyMMdd hh:mm:ss
    * @param numFromKafka records from kafka
    * @param numIntoKafka records into kafka
    */
  def saveFlowData(topicName: String,
                   currentTime: String,
                   numFromKafka: Long,
                   numIntoKafka: Long): Unit = {
    val dt: String = currentTime.substring(0, 8)
    val sql = "INSERT INTO `etl_flow_monitor`(dt,topic,num_from_kafka,num_into_kafka) VALUES(?,?,?,?) ON DUPLICATE " +
      "KEY UPDATE num_from_kafka = ?,num_into_kafka = ?"
    DBUtil.saveOrUpdate(
      sql,
      dt.asInstanceOf[AnyRef],
      topicName.asInstanceOf[AnyRef],
      numFromKafka.asInstanceOf[AnyRef],
      numIntoKafka.asInstanceOf[AnyRef],
      numFromKafka.asInstanceOf[AnyRef],
      numIntoKafka.asInstanceOf[AnyRef]
    )
  }

  /**
    *
    * @param topic topic
    * @param sparkSession SparkSession
    * @return
    */
  def transferTopic(topic: String,
                    sparkSession: SparkSession): sql.DataFrame = {
    val filterSQL = SparkService.topicValidation.value.getOrElse(topic, null)
    val filteredTbl =
      if (filterSQL != null) s"${Constants.TBL_FILTERED_DATA} WHERE $filterSQL"
      else Constants.TBL_FILTERED_DATA
    val transferSQL = SparkService.transferFieldSQL.value.getOrElse(topic, null) //transformation
    val resultDF =
      if (transferSQL != null)
        sparkSession.sql(s"SELECT $transferSQL FROM $filteredTbl")
      else null
    resultDF.createOrReplaceTempView(Constants.TBL_TRANSFER_DATA)
    val fieldSQL = SparkService.outputFields.value.getOrElse(topic, null)
    if (fieldSQL != null)
      sparkSession.sql(s"SELECT $fieldSQL FROM " + Constants.TBL_TRANSFER_DATA)
    else resultDF
  }
}
