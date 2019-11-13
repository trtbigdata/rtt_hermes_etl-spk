package com.hupu.hermes.launch

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hupu.hermes.service.SparkService
import com.hupu.hermes.service.SparkService.ipDbBC
import com.hupu.hermes.utils.UDFs.ipLong
import com.hupu.hermes.utils.{ConfigUtil, Constants, KafkaSink, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks

/**
  * @author yanjiajun
  **/
object HermesETLLaunch {
  val LOG: Logger = LoggerFactory.getLogger(TopicStreamingLaunch.getClass)

  @volatile var sparkConf: SparkConf = _
  @volatile var sparkSession: SparkSession = _
  val dateFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd hh:mm:ss")

  def main(args: Array[String]): Unit = {
    val appName = Constants.APP_NAME
    val inKafkaTopics =
      Constants.IN_KAFKA_TOPIC.split(",").toSet
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .registerKryoClasses(Array(SparkService.getClass))
    if (StringUtils.isNoneBlank() && "1".equals(Constants.IF_LOCAL))
      sparkConf.setMaster("local[*]")
    val ssc = new StreamingContext(
      sparkConf,
      Seconds(Constants.STREAMING_DURATION.toLong)
    )
    sparkSession = SparkUtil.getSparkSession(sparkConf)
    val kafkaDStream = SparkUtil.createDirectStream(ssc, inKafkaTopics)
    //从配置中获取解析topic,解析sql,写入topic
    val topic = Constants.IN_KAFKA_TOPIC
    val outputTopic = ConfigUtil.getProperty("etl." + topic + ".output.topic")
    //初始化累加器
    val accumulatorMap: Map[String, LongAccumulator] =
      SparkService.initAccumulator(sparkSession, topic)
    val kafkaProducer: Broadcast[KafkaSink[String, String]] =
      SparkService.kafkaProducer(sparkSession)

    val recordsFromKafka = accumulatorMap(Constants.ACCU_FROM_KAFKA)
    val recordsIntoKafka = accumulatorMap(Constants.ACCU_INTO_KAFKA) //注册UDF
    if (outputTopic == null) return
    val ipDB = ipDbBC(sparkSession)
    kafkaDStream.foreachRDD { rdd =>
      {
        recordsFromKafka.add(rdd.count())
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
          .filter(a => a.topic == topic && a.value().length > 0)
          .map(x => {
            var record = JSON.parseObject(x.value().toString)
            record = parseABTest(record)
            record = parseTimeStamp(record)
            record = registerParseRegion(record, ipDB)
            record = parseJson(record)
            record
          })
          .foreachPartition(p => {
            p.foreach(record => {
              kafkaProducer.value.send(outputTopic, record.toJSONString)
            })
          })
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        recordsIntoKafka.add(rdd.count())
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

  /**
    * 解析ab test
    *
    * @return
    */
  def parseABTest(record: JSONObject): JSONObject = {
    //{"control_float_ball_banner":{"vid":"1286","val":"1"},"control_grand_prize":{"vid":"1286","val":"4"}}
    import scala.collection.JavaConversions._
    try {
      val mapTypes: fastjson.JSONObject = record.getJSONObject("ab")
      for (obj <- mapTypes.keySet) {
        val map: fastjson.JSONObject =
          JSON.parseObject(mapTypes.get(obj).toString)
        record.put(obj, map.get("val").toString)
        record.remove("ab")
      }
    } catch {
      case _: Exception => record
    }
    record
  }

  /**
    * 解析时间戳
    *
    * @return
    */
  def parseTimeStamp(record: JSONObject): JSONObject = {
    var localDateTime: LocalDateTime = null
    try {
      val timestampLong = record.getString("server_time").toLong
      localDateTime = Instant
        .ofEpochMilli(timestampLong)
        .atZone(ZoneOffset.ofHours(8))
        .toLocalDateTime
      if (record.getString("server_time").length == 10) {
        localDateTime = Instant
          .ofEpochSecond(timestampLong)
          .atZone(ZoneOffset.ofHours(8))
          .toLocalDateTime
      }
      record.put("year", localDateTime.getYear)
      record.put("month", localDateTime.getMonth.getValue)
      record.put("day", localDateTime.getDayOfMonth)
      record.put("hour", localDateTime.getHour)
      record.put("minute", localDateTime.getMinute)
      record
    } catch {
      case _: Exception => record
    }
  }

  /**
    *
    * @param ipDB         ipDataBase
    * @return
    */
  def registerParseRegion(
    record: JSONObject,
    ipDB: Broadcast[Array[(Long, Long, String, String, String)]]
  ): JSONObject =
    try {
      val ipResource = ipDB.value
      val ipStr = record.getString("ip")
      val ipNum = ipLong(ipStr)
      //二分查找法口诀：上下循环寻上下，左移右移寻中间
      //命中标记
      var target = -1
      //开始下标
      var start = 0
      //结束下标
      var end = ipResource.length - 1
      val loop = new Breaks;
      loop.breakable {
        while (start <= end) {
          val middle = (start + end) / 2
          if (ipNum >= ipResource(middle)._1 && ipNum <= ipResource(middle)._2) {
            target = middle
            loop.break;
          }
          if (ipNum > ipResource(middle)._2) start = middle
          if (ipNum < ipResource(middle)._1) end = middle
        }
      }
      if (target < 0) {
        record.put("cip", ipStr)
        record.put("country", "unknown")
        record.put("province", "unknown")
        record.put("city", "unknown")
      } else {
        record.put("cip", ipStr)
        record.put("country", ipResource(target)._3)
        record.put("province", ipResource(target)._4)
        record.put("city", ipResource(target)._5)
      }
      record
    } catch {
      case _: Exception =>
        record.put("cip", record.getString("ip"))
        record.put("country", "unknown")
        record.put("province", "unknown")
        record.put("city", "unknown")
        record
    }

  /**
    *
    * @param record
    * @return
    */
  def parseJson(record: JSONObject): JSONObject = {
    val extJson: fastjson.JSONObject = record.getJSONObject("ext")
    import scala.collection.JavaConversions._
    if (extJson != null) {
      for (entry <- extJson.entrySet) {
        record.put("ext_" + entry.getKey, entry.getValue)
      }
    }
    val metaJson: fastjson.JSONObject = record.getJSONObject("meta")
    import scala.collection.JavaConversions._
    for (entry <- metaJson.entrySet) {
      if (entry.getKey.equals("pos") || entry.getKey.equals("line_number")) {
        record.put("meta_" + entry.getKey, entry.getValue.toString.toLong)
      } else {
        record.put("meta_" + entry.getKey, entry.getValue)
      }
    }
    record.remove("ext")
    record.remove("meta")
    record
  }
}
