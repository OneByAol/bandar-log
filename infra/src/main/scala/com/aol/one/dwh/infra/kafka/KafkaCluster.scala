package com.aol.one.dwh.infra.kafka

import java.util.Properties

import com.aol.one.dwh.infra.config.KafkaConfig
import com.google.common.cache.CacheBuilder
import kafka.common.TopicAndPartition
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.{CommonClientConfigs, admin}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.SystemTime
import scalacache.ScalaCache
import scalacache.guava.GuavaCache
import scalacache.memoization._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

abstract class KafkaCluster extends AutoCloseable {

  type Offset = Long

  val adminClient: AdminClient
  val consumer: KafkaConsumer[String, String]
  val kafkaAwaitingTimeout: Duration
  val cacheInvalidationTimeout: Duration

  implicit val scalaCache = ScalaCache(
    GuavaCache(
      CacheBuilder
        .newBuilder()
        .build[String, Object]
    )
  )

  /**
    * Get and cache partition and offset metadata for particular consumer group
    * @param groupId  - consumer group id
    * @return
    */
  private def getKafkaMetadata(groupId: String): Future[Map[TopicPartition, OffsetAndMetadata]] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val f: Future[Map[TopicPartition, OffsetAndMetadata]] = Future {
      adminClient
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata()
        .get()
    }.map(_.asScala.toMap)

    memoize(cacheInvalidationTimeout)(f)
  }

  /**
    * Get offsets for particular consumer group
    * @param groupId  - consumer group id
    * @param topics   - topic names
    * @return
    */
  def getConsumerOffsets(groupId: String, topics: Set[String]): Either[Throwable, Map[TopicAndPartition, Offset]] = {
    val metadata = Try(Await.result(getKafkaMetadata(groupId), kafkaAwaitingTimeout))
    Either.cond(
      metadata.isSuccess,
      metadata
        .get
        .filter { case (tp, _) => topics.contains(tp.topic()) }
        .map { case (tp, offsetAndMetadata) => (new TopicAndPartition(tp), offsetAndMetadata.offset()) },
      metadata.failed.get
    )
  }

  /**
    * Get latest records offsets for particular consumer group.
    * Consumer API method guarantees it does not change the current consumer position of the partitions.
    * See [[org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(java.util.Collection)]]
    * @param groupId  - consumer group id
    * @param topics   - topic names
    * @return
    */
  def getLatestOffsets(groupId: String, topics: Set[String]): Either[Throwable, Map[TopicAndPartition, Offset]] = {
    val metadata = Try(Await.result(getKafkaMetadata(groupId), kafkaAwaitingTimeout))
    Either.cond(
      metadata.isSuccess,
      {
        val topicAndPartitions =
          metadata
            .get
            .keySet
            .filter { tp => topics.contains(tp.topic()) }
            .map {tp => new TopicPartition(tp.topic, tp.partition)}
            .asJavaCollection
        val offsets: Map[TopicAndPartition, Long] =
          consumer
            .endOffsets(topicAndPartitions)
            .asScala
            .toMap
            .map { case (tp, offset) => (new TopicAndPartition(tp), offset.longValue()) }

        offsets
      },
      metadata.failed.get
    )
  }

  override def close(): Unit = {
    consumer.close()
    adminClient.close()
  }
}

object KafkaCluster {

  private val defaultKafkaResponseTimeout = 1 second
  private val defaultCachingTime = 10 seconds

  def apply(config: KafkaConfig): KafkaCluster = {
    val brokers: String = {

      val zkClient = KafkaZkClient(config.zookeeperQuorum, isSecure = false, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, new SystemTime())
      val endpoints = zkClient.getAllBrokersInCluster.flatMap(_.endPoints)
      val brokersFromZK = endpoints.map{ endpoint => s"${endpoint.host}:${endpoint.port}"}.mkString(",")
      zkClient.close()

      config.brokers.getOrElse(brokersFromZK)
    }

    new KafkaCluster {
      override val adminClient: AdminClient = createAdminClient(brokers)
      override val consumer: KafkaConsumer[String, String] = createConsumer(brokers)
      override val kafkaAwaitingTimeout: Duration = config.kafkaResponseTimeout.getOrElse(defaultKafkaResponseTimeout)
      override val cacheInvalidationTimeout: Duration = config.cacheResultsTime.getOrElse(defaultCachingTime)
    }
  }

  private def createAdminClient(brokers: String): admin.AdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)

    AdminClient.create(props)
  }

  private def createConsumer(brokers: String): KafkaConsumer[String, String] = {
    val properties = new Properties()
    val deserializer = (new StringDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"bandarlog-${System.currentTimeMillis()}")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

    new KafkaConsumer(properties)
  }
}
