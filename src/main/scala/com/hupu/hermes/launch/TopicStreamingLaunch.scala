package com.hupu.hermes.launch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.hupu.hermes.impl.{TopicTransformation, TopicValidation}
import com.hupu.hermes.service.{SchemaService, SparkService}
import com.hupu.hermes.utils.{ConfigUtil, Constants, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author yanjiajun
  **/
object TopicStreamingLaunch {
  val LOG: Logger = LoggerFactory.getLogger(TopicStreamingLaunch.getClass)

  @volatile var sparkConf: SparkConf = _
  @volatile var sparkSession: SparkSession = _
  // 格式化时间
  val dateFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd hh:mm:ss")

  def main(args: Array[String]): Unit = {
    // 取出配置文件中switch的值
    val switch: String = Constants.SWITCH
    // 取出配置文件中app_name的值
    val appName = Constants.APP_NAME
    // 取出配置文件中inKafka.topics的值
    val inKafkaTopics =
      Constants.IN_KAFKA_TOPIC.split(",").toSet //toSet转化成Set进行去重
    // 初始化sparkConf，并使用registerKryoClasses来进行Kryo序列化，优化的一种方式
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .registerKryoClasses(Array(SparkService.getClass))
    /**
      * 这里只是将SparkService这一个类进行了Kryo序列化
      * SparkService类需要实现Serializable接口
      * 因为SparkService是记录要广播出去的数据，需要消耗网络性能和磁盘io，所以只对这种
      * 需要经常磁盘IO的数据进行Kryo序列化
      */

    if (StringUtils.isNoneBlank() && "1".equals(Constants.IF_LOCAL))
      // local[*]: 这种模式直接帮你按照cpu最多cores来设置线程数了
      sparkConf.setMaster("local[*]")
    val ssc = new StreamingContext(
      sparkConf,
      Seconds(Constants.STREAMING_DURATION.toLong)
    )
    //Spark2.0只是将DataSet和DataFrame统一了，意思是SparkCore和SparkSql统一成SparkSession，而SparkStreaming上下文没变
    sparkSession = SparkUtil.getSparkSession(sparkConf)
    val kafkaDStream = SparkUtil.createDirectStream(ssc, inKafkaTopics)
    //从配置中获取解析topic,解析sql,写入topic
    val topic = Constants.IN_KAFKA_TOPIC
    var outputTopic = ConfigUtil.getProperty("etl." + topic + ".output.topic")
    val topicParseSQL = ConfigUtil.getProperty("etl." + topic + ".sql")
    //初始化累加器
    val accumulatorMap: Map[String, LongAccumulator] =
      SparkService.initAccumulator(sparkSession, topic)
    val recordsFromKafka = accumulatorMap(Constants.ACCU_FROM_KAFKA)
    val recordsIntoKafka = accumulatorMap(Constants.ACCU_INTO_KAFKA) //注册UDF
    SparkService.registerUDFs(sparkSession)
    if (switch != "" && switch.toLong == 1) {
      val topicValidate = new TopicValidation(appName, topic)
      topicValidate.bcTopicValidationRule(topic) //广播校验SQL
      val topicTransfer = new TopicTransformation(appName, topic)
      topicTransfer.bcTopicTransferRule(topic) //广播转换SQL
      outputTopic = SparkService.inputOutputTopic.value(topic)
    }
    if (outputTopic == null) return
    val schema: StructType = SchemaService.createTopicSchema(topic)
    kafkaDStream.foreachRDD { rdd =>
      {
        recordsFromKafka.add(rdd.count())
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val jsonRDD: RDD[Row] = rdd
          .filter(a => a.topic == topic && a.value().length > 0)
          .map(x => {
            val obj = JSON.parseObject(x.value().toString)
            Row.fromSeq(SchemaService.createSeq(obj, schema))
          })
          .repartition(180)
        val rdd2DF = sparkSession.createDataFrame(jsonRDD, schema)
        rdd2DF.createOrReplaceTempView(Constants.TBL_FILTERED_DATA)
        val lastResultDF: DataFrame =
          if (switch.toLong == 0 && topicParseSQL != null && !rdd2DF.rdd
                .isEmpty()) {
            sparkSession.sql(s"$topicParseSQL")
          } else if (switch.toLong == 1) {
            SparkService.transferTopic(topic, sparkSession)
          } else {
            rdd2DF
          }
        lastResultDF
          .selectExpr("to_json(struct(*)) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", Constants.OUT_KAFKA_SERVERS)
          .option("kafka.compression.type", "snappy")
          .option("kafka.batch.size", 65536)
          .option("topic", outputTopic)
          .save()
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        recordsIntoKafka.add(lastResultDF.count())
        val currentTime: String = LocalDateTime.now.format(dateFormat)
        val min = currentTime.substring(12, 14).toLong
        if (min % 5 == 0) {
          SparkService.saveFlowData(
            topic,
            currentTime,
            recordsFromKafka.value,
            recordsIntoKafka.value
          ) //5分钟更新一次;
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
