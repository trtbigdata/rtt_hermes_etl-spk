package com.hupu.hermes.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.mutable.ArrayBuffer

/**
  * @author yanjiajun
  */
object GroupConcat extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType().add("x", StringType)
  def bufferSchema: StructType =
    new StructType().add("buff", ArrayType(StringType))
  def dataType: StringType = StringType
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, ArrayBuffer.empty[String])
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0))
      buffer.update(0, buffer.getSeq[String](0) :+ input.getString(0))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getSeq[String](0) ++ buffer2.getSeq[String](0))
  }

  def evaluate(buffer: Row): UTF8String =
    UTF8String.fromString(buffer.getSeq[String](0).mkString(","))
}
