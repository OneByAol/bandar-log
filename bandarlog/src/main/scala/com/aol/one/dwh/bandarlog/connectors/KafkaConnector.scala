/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.bandarlog.connectors.KafkaConnector._
import com.aol.one.dwh.infra.config.Topic
import com.aol.one.dwh.infra.kafka.KafkaCluster
import com.aol.one.dwh.infra.util.LogTrait
import kafka.common.TopicAndPartition

object KafkaConnector {
  type Offset = Long
  type KafkaPartitions = Set[TopicAndPartition]
  type KafkaHeads = Map[TopicAndPartition, Offset]
  type KafkaOffsets = Map[TopicAndPartition, Offset]
  type KafkaState = (KafkaHeads, KafkaOffsets)
}

/**
  * Kafka Connector
  *
  * Provides Kafka cluster API by kafka config
  */
class KafkaConnector(kafkaCluster: KafkaCluster) extends LogTrait {

  def getKafkaState(topic: Topic): Option[KafkaState] =
    for {
      heads      <- getHeads(topic)
      offsets    <- getOffsets(topic)
    } yield (heads, offsets)

  def getHeads(topic: Topic): Option[KafkaHeads] = {
    kafkaCluster.getLatestOffsets(topic.groupId, topic.values) match {
      case Left(l) =>
        logger.error(s"Cannot obtain leaders offsets for topic:[${topic.values}], cause {}", l.toString)
        None
      case Right(r) => Some(r.map { case (key, value) => key -> value })
    }
  }

  def getOffsets(topic: Topic): Option[KafkaOffsets] = {
    kafkaCluster.getConsumerOffsets(topic.groupId, topic.values ) match {
      case Left(l) =>
        logger.error(s"Cannot obtain consumers offsets for topic:[${topic.values}], cause {}", l.toString)
        None
      case Right(r) => Some(r)
    }
  }
}
