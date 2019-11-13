package com.hupu.hermes.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.hupu.hermes.utils.ConfigUtil
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object SchemaService extends Serializable {

  /**
    * 限定json最多两层
    * @param topic topic
    * @return
    */
  def createTopicSchema(topic: String): StructType = {
    val fieldArr = ArrayBuffer[StructField]()
    val schemaStr = ConfigUtil.getProperty("etl." + topic + ".schema")
    val schemaObj = JSON.parseObject(schemaStr)
    val iter = schemaObj.keySet().iterator
    while (iter.hasNext) {
      val inputField: String = iter.next()
      val inputType: String = schemaObj.getString(inputField)
      if (!inputType.contains("{")) {
        val dataType = getDatatype(inputType)
        fieldArr += StructField(inputField, dataType)
      } else {
        val obj = JSON.parseObject(inputType)
        val it = obj.keySet().iterator
        while (it.hasNext) {
          val field = it.next()
          val tp = obj.get(field).toString
          val dataTp = getDatatype(tp)
          fieldArr += StructField(inputField + "__" + field, dataTp)
        }
      }
    }
    new StructType(fieldArr.toArray)
  }

  /**
    *
    * @param inputType inputType
    * @return
    */
  def getDatatype(inputType: String): DataType = {
    inputType match {
      case "String"  => StringType
      case "Integer" => IntegerType
      case "Long"    => LongType
      case "Boolean" => BooleanType
      case "Double"  => DoubleType
      case _         => StringType
    }
  }

  /**
    *
    * @param field String
    * @param dataType DataType
    * @param obj JSONObject
    * @param seq Seq
    * @return
    */
  def perfectSeq(field: String,
                 dataType: DataType,
                 obj: JSONObject,
                 seq: Seq[Any]): Seq[Any] = {
    dataType match {
      case StringType  => seq :+ obj.getString(field)
      case IntegerType => seq :+ obj.getIntValue(field)
      case LongType    => seq :+ obj.getLongValue(field)
      case BooleanType => seq :+ obj.getBooleanValue(field)
      case DoubleType  => seq :+ obj.getDoubleValue(field)
      case _           => seq :+ obj.getString(field)
    }
  }

  /**
    * @param obj    Object
    * @param schema Schema
    * @return
    */
  def createSeq(obj: JSONObject, schema: StructType): Seq[Any] = {
    var seq: Seq[Any] = Seq()
    schema.fields.foreach(s => {
      val dataType: DataType = s.dataType
      val field: String = s.name
      if (!field.contains("__")) {
        seq = perfectSeq(field, dataType, obj, seq)
      } else {
        val fieldArr = field.split("__")
        val column = fieldArr(1)
        val json = JSON.parseObject(obj.getString(fieldArr(0)))
        if (json != null) {
          seq = perfectSeq(column, dataType, json, seq)
        } else {
          seq = seq :+ null
        }
      }
    })
    seq
  }
}
