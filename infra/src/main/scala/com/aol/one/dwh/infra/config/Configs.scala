/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.config

import scala.concurrent.duration.Duration

/**
  * Kafka configuration
  *
  * @param zookeeperQuorum - zookeeper host
  * @param brokers         - kafka brokers list
  *
  */
case class KafkaConfig(zookeeperQuorum: String, brokers: Option[String])

/**
  * Glue client configuration
  *
  * @param region             - aws region
  * @param dbname             - database name
  * @param accessKey          - access key provided by AWS account
  * @param secretKey          - secret key provided by AWS account
  * @param maxFetchSize       - the maximum number of partitions to return in a single response
  * @param segmentTotalNumber - total number of segments in glue table, also sets the level of parallelism of computation (number of threads)
  * @param maxWaitTimeout     - timeout to complete glue request
  */
case class GlueConfig(
  region: String,
  dbname: String,
  accessKey: String,
  secretKey: String,
  maxFetchSize: Int,
  segmentTotalNumber: Int,
  maxWaitTimeout: Duration
)

/**
  * Datadog Config
  *
  * @param host - datadog host
  */
case class DatadogConfig(host: Option[String])

/**
  * Scheduler Config
  *
  * @param delayPeriod      - delay in milliseconds before task is to be executed
  * @param schedulingPeriod - time in milliseconds between successive task executions
  */
case class SchedulerConfig(delayPeriod: Duration, schedulingPeriod: Duration)

/**
  * Connector Config
  *
  * @param connectorType - connector type (vertica, presto, kafka)
  * @param configId      - config id for selected connector type
  * @param tag           - reporter tag
  */
case class ConnectorConfig(connectorType: String, configId: String, tag: String)

/**
  * Reporter Config
  *
  * @param reporterType  - reporter type (like datadog)
  * @param configId      - config id for selected reporter type
  */
case class ReporterConfig(reporterType: String, configId: String)

/**
  * Report Config
  *
  * @param prefix   - reporting metric prefix
  * @param interval - reporting interval in seconds
  */
case class ReportConfig(prefix: String, interval: Int)

/**
  * Kafka topic
  *
  * @param id      - topic id
  * @param values  - topic values
  * @param groupId - group id
  *
  */
case class Topic(id: String, values: Set[String], groupId: String)

trait Table {
  val tableName: String
  val columns: List[String]
  val formats: List[String]
}

/**
  * Pair of Sql table and column
  */
case class NumericColumn(tableName: String, columnName: String) extends Table {
  override val columns: List[String] = List(columnName)
  override val formats: List[String] = Nil
}

/**
  * Pair of Sql table and partitions
  */
case class NonnumericColumn(tableName: String, partitions: List[Partition])  extends Table {
  override val columns: List[String] = partitions.map(_.column)
  override val formats = partitions.map(_.format)
}

case class Partition(column: String, format: String)

/**
  * Reporter Tag
  *
  * @param key   - tag key
  * @param value - tag value
  */
case class Tag(key: String, value: String)

/**
  * Jdbc configuration
  */
case class JdbcConfig(
  host: String,
  port: Int,
  dbName: String,
  username: String,
  password: String,
  schema: String,
  useSsl: Boolean = true,
  maxPoolSize: Int = 10,
  connectionTimeout: Long = 60000
)
