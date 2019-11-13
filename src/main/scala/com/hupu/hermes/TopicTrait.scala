package com.hupu.hermes

trait TopicTrait extends Serializable {

  /**
    * 广播校验规则
    */
  def bcTopicValidationRule(topic: String)

  /**
    * 广播转换规则
    */
  def bcTopicTransferRule(topic: String)

}
