/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.config.Topic
import com.aol.one.dwh.infra.kafka.KafkaCluster
import kafka.common.TopicAndPartition
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class KafkaConnectorTest extends FunSuite with MockitoSugar {

  private val kafkaCluster = mock[KafkaCluster]
  private val kafkaConnector = new KafkaConnector(kafkaCluster)

  private val topic = Topic("topic_id", Set("topic_1", "topic_2"), "group_id")

  test("return kafka heads") {
    val headsValues = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val partitions: Either[Throwable, Set[TopicAndPartition]] = Right(headsValues.keySet)
    val heads: Either[Throwable, Map[TopicAndPartition, Long]] = Right(headsValues)

    when(kafkaCluster.getLatestOffsets(topic.groupId, topic.values)).thenReturn(heads)

    val resultHeads = kafkaConnector.getHeads(topic)

    assert(resultHeads.nonEmpty)
    assert(resultHeads.get == Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    ))
  }

  test("return none if can't retrieve heads") {
    val headsValues = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val heads: Either[Throwable, Map[TopicAndPartition, Long]] = Left(new Throwable)

    when(kafkaCluster.getLatestOffsets(topic.groupId, topic.values)).thenReturn(heads)

    val resultHeads = kafkaConnector.getHeads(topic)

    assert(resultHeads.isEmpty)
  }

  test("return kafka offsets") {
    val offsetValues = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val offsets: Either[Throwable, Map[TopicAndPartition, Long]] = Right(offsetValues)

    when(kafkaCluster.getConsumerOffsets(topic.groupId, topic.values)).thenReturn(offsets)

    val resultOffsets = kafkaConnector.getOffsets(topic)

    assert(resultOffsets.nonEmpty)
    assert(resultOffsets.get == offsetValues)
  }

  test("return none if can't retrieve offsets") {
    val offsetValues = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val offsets: Either[Throwable, Map[TopicAndPartition, Long]] = Left(new Throwable)

    when(kafkaCluster.getConsumerOffsets(topic.groupId, topic.values)).thenReturn(offsets)

    val resultOffsets = kafkaConnector.getOffsets(topic)

    assert(resultOffsets.isEmpty)
  }

  test("return kafka state") {
    val headsValues = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val offsetValues = Map(
      TopicAndPartition("topic_1", 1) -> 4L,
      TopicAndPartition("topic_2", 2) -> 5L,
      TopicAndPartition("topic_3", 3) -> 6L
    )

    val heads: Either[Throwable, Map[TopicAndPartition, Long]] = Right(headsValues)
    val offsets: Either[Throwable, Map[TopicAndPartition, Long]] = Right(offsetValues)

    when(kafkaCluster.getLatestOffsets(topic.groupId, topic.values)).thenReturn(heads)
    when(kafkaCluster.getConsumerOffsets(topic.groupId, topic.values)).thenReturn(offsets)

    val resultState = kafkaConnector.getKafkaState(topic)

    val expectedHeads = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )

    val expectedOffsets = Map(
      TopicAndPartition("topic_1", 1) -> 4L,
      TopicAndPartition("topic_2", 2) -> 5L,
      TopicAndPartition("topic_3", 3) -> 6L
    )
    assert(resultState.nonEmpty)
    assert(resultState.get == (expectedHeads, expectedOffsets))
  }
}
