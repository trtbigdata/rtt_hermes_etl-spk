package com.hupu.hermes.utils

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.control.Breaks

/**
  * @author yanjiajun
  */
object UDFs {
  val LOG: Logger = LoggerFactory.getLogger(UDFs.getClass)

  /**
    *
    * @param ip IP
    * @return
    */
  def ipLong(ip: String): Long = {
    val ipArray = ip.split("\\.")
    var ipNum = ipArray(0).toLong
    for (i <- 1 until ipArray.length) {
      ipNum = (ipNum << 8) + ipArray(i).toLong
    }
    ipNum
  }

  /**
    *
    * @param sparkSession SparkSession
    * @param ipDB         ipDataBase
    * @return
    */
  def registerParseRegion(
      sparkSession: SparkSession,
      ipDB: Broadcast[Array[(Long, Long, String, String, String)]])
    : UserDefinedFunction =
    sparkSession.udf.register(
      Constants.PARSE_REGION,
      (ipStr: String) => {
        try {
          //        SparkService.recordsFromKafka.add(1)
          val ipResource = ipDB.value
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
          if (target < 0)
            Map("cip" -> ipStr,
                "country" -> "unknown",
                "province" -> "unknown",
                "city" -> "unknown")
          else
            Map("cip" -> ipStr,
                "country" -> ipResource(target)._3,
                "province" -> ipResource(target)._4,
                "city" -> ipResource(target)._5)
        } catch {
          case _: Exception =>
            Map("cip" -> ipStr,
                "country" -> "unknown",
                "province" -> "unknown",
                "city" -> "unknown")
        }
      }
    )

  /**
    * 解析path
    *
    * @return
    */
  def parsePath: String => Map[String, String] = path => {
    Map("itf" -> "itf",
        "channel" -> "channel",
        "_imei" -> "_imei",
        "ver" -> "ver",
        "tid" -> "tid")
  }

  /**
    * 解析时间戳
    *
    * @return
    */
  def parseTimeStamp: String => Map[String, Long] = timestampStr => {
    var localDateTime: LocalDateTime = null
    try {
      val timestampLong = timestampStr.toLong
      localDateTime = Instant
        .ofEpochMilli(timestampLong)
        .atZone(ZoneOffset.ofHours(8))
        .toLocalDateTime
      if (timestampStr.length == 10) {
        localDateTime = Instant
          .ofEpochSecond(timestampLong)
          .atZone(ZoneOffset.ofHours(8))
          .toLocalDateTime
      }
    } catch {
      case _: Exception => localDateTime = LocalDateTime.now
    }
    Map(
      "time" -> timestampStr.toLong,
      "year" -> localDateTime.getYear,
      "month" -> localDateTime.getMonth.getValue,
      "day" -> localDateTime.getDayOfMonth,
      "hour" -> localDateTime.getHour,
      "minute" -> localDateTime.getMinute
    )
  }

  /**
    * 性别转换
    */
  def parseGender: Int => String = gender => {
    var sex = "ukn"
    if (gender == 0) sex = "女" else if (gender == 1) sex = "男"
    sex
  }

  /**
    * 解析ab test
    *
    * @return
    */
  def parseABTest: String => Map[String, String] = ab_data => {
    //{"control_float_ball_banner":{"vid":"1286","val":"1"},"control_grand_prize":{"vid":"1286","val":"4"}}
    import scala.collection.JavaConversions._
    val mapRes = mutable.LinkedHashMap[String, String]()
    try {
      val mapTypes: fastjson.JSONObject = JSON.parseObject(ab_data)
      for (obj <- mapTypes.keySet) {
        val map: fastjson.JSONObject = JSON.parseObject(mapTypes.get(obj).toString)
        mapRes += (obj -> map.get("val").toString)
      }
    }catch {
      case _: Exception => mapRes.toMap
    }
    mapRes.toMap
  }
}
