/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.config

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

/**
  * Kafka configuration
  *
  * @param brokers                - kafka brokers list
  * @param kafkaResponseTimeout   - for how long to wait for response from Kafka
  * @param cacheResultsTime       - for how long to cache Kafka metadata
  *
  */
case class KafkaConfig(
  brokers: String,
  kafkaResponseTimeout: Option[Duration],
  cacheResultsTime: Option[Duration]
)

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

/**
  * Sql table with columns
  * @param table - table name
  * @param columns - columns' names
  * @param formats - format of columns (e.g., yyyy, MM, dd, HH:mm:ss) for datetime column
  * @param tag - an alternative tag to use for metrics instead of default, which is a table name
  */
case class Table(
  table: String,
  columns: List[String],
  filters: Option[List[Filter]],
  formats: Option[List[String]],
  tag: Option[String],
  timestampType: Option[String] = None
)

/**
 * Filter for sql table
 * @param key - column name
 * @param value - column value
 * @param quoted - if column is a string it should be quoted
 */
case class Filter(key: String, value: String, quoted: Boolean, dynamic: Boolean)

/**
  * Partition column
  *
  * @param columnName - partition name
  * @param columnFormat - date type column format
  */
case class DatetimePatition(columnName: String, columnFormat: String)

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
