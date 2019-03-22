/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.config

import com.aol.one.dwh.infra.parser.ColumnParser
import com.typesafe.config.{Config, ConfigObject}
import scala.collection.JavaConversions._
import scala.concurrent.duration._

object RichConfig {

  implicit class RichOptionalConfig(val underlying: Config) extends AnyVal {

    def getOptionalString(path: String): Option[String] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }

    def getOptionalInt(path: String): Option[Int] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
        Some(underlying.getInt(path))
      } else {
        None
      }

    def getOptionalLong(path: String): Option[Long] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
        Some(underlying.getLong(path))
      } else {
        None
      }

    def getOptionalBoolean(path: String): Option[Boolean] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
        Some(underlying.getBoolean(path))
      } else {
        None
      }

    def getOptionalStringList(path: String): Option[List[String]] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
      Some(underlying.getStringList(path).toList)
    } else {
      None
    }

    def getOptionalObjectList(path: String): Option[List[_ <: ConfigObject]] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
        Some(underlying.getObjectList(path).toList)
      } else {
        None
      }

    def getOptionalConfig(path: String): Option[Config] =
      if (underlying.hasPath(path) && !underlying.getIsNull(path)) {
        Some(underlying.getConfig(path))
      } else {
        None
      }
  }

  implicit class RichConfig(val underlying: Config) extends AnyRef {

    def isEnabled: Boolean = {
      underlying.getOptionalBoolean("enabled").getOrElse(false)
    }

    def getBandarlogType: String = {
      underlying.getString("bandarlog-type")
    }

    def getJdbcConfig(verticaConfigId: String): JdbcConfig = {
      val conf = underlying.getConfig(verticaConfigId)

      var jdbcConfig = JdbcConfig(
        host     = conf.getString("host"),
        port     = conf.getInt("port"),
        dbName   = conf.getString("dbname"),
        username = conf.getString("username"),
        password = conf.getString("password"),
        schema   = conf.getString("schema")
      )

      // apply overrides
      jdbcConfig = conf.getOptionalBoolean("use-ssl").map(ssl => jdbcConfig.copy(useSsl = ssl)).getOrElse(jdbcConfig)
      jdbcConfig = conf.getOptionalInt("max.pool.size").map(ps => jdbcConfig.copy(maxPoolSize = ps)).getOrElse(jdbcConfig)
      jdbcConfig = conf.getOptionalLong("connection.timeout.ms").map(t => jdbcConfig.copy(connectionTimeout = t)).getOrElse(jdbcConfig)

      jdbcConfig
    }

    def getDatadogConfig(configId: String): DatadogConfig = {
      val datadogConfig = underlying.getConfig(configId)

      DatadogConfig(
        datadogConfig.getOptionalString("host")
      )
    }

    def getSchedulerConfig: SchedulerConfig = {
      val schedulerConfig = underlying.getConfig("scheduler")

      SchedulerConfig(
        schedulerConfig.getInt("delay.seconds").seconds,
        schedulerConfig.getInt("scheduling.seconds").seconds
      )
    }

    def getKafkaConfig(configId: String): KafkaConfig = {
      val kafkaConfig = underlying.getConfig(configId)

      KafkaConfig(
        kafkaConfig.getString("zk-quorum"),
        kafkaConfig.getOptionalString("brokers")
      )
    }

    def getGlueConfig(configId: String): GlueConfig = {
      val glueConfig = underlying.getConfig(configId)

      GlueConfig(
        glueConfig.getString("region"),
        glueConfig.getString("dbname"),
        glueConfig.getString("access.key"),
        glueConfig.getString("secret.key"),
        glueConfig.getInt("fetch.size"),
        glueConfig.getInt("segment.total.number"),
        glueConfig.getInt("maxwait.timeout.seconds").seconds
      )
    }


    def getReporters: List[ReporterConfig] = {
      underlying.getOptionalObjectList("reporters").getOrElse(Nil).map { obj =>
        ReporterConfig(
          obj.toConfig.getString("type"),
          obj.toConfig.getString("config-id")
        )
      }
    }

    def getReportConfig: ReportConfig = {
      val config = underlying.getConfig("report")

      ReportConfig(
        config.getString("prefix"),
        config.getInt("interval.sec")
      )
    }

    def getConnector: String = {
      underlying.getString("connector")
    }

    def getInConnector: ConnectorConfig = {
      underlying.getOptionalConfig("in-connector").map { connectorConfig =>
        ConnectorConfig(
          connectorConfig.getString("type"),
          connectorConfig.getString("config-id"),
          connectorConfig.getString("tag")
        )
      }.getOrElse(ConnectorConfig("", "", ""))
    }

    def getOutConnectors: Seq[ConnectorConfig] = {
      underlying.getOptionalObjectList("out-connectors").getOrElse(List.empty).map { obj =>
        ConnectorConfig(
          obj.toConfig.getString("type"),
          obj.toConfig.getString("config-id"),
          obj.toConfig.getString("tag")
        )
      }
    }

    def getMetrics: Seq[String] = {
      underlying.getStringList("metrics")
    }

    def getKafkaTopics: Seq[Topic] = {
      underlying.getObjectList("topics").map { obj =>
        val topicConfig = obj.toConfig
        val topicId = topicConfig.getString("topic-id")
        val groupId = topicConfig.getString("group-id")
        val topicValues = topicConfig.getStringList("topic").toSet

        Topic(topicId, topicValues, groupId)
      }
    }

    def getTables: Seq[(Table, Table)] = {
      underlying.getObjectList("tables").map { obj =>
        val withFormat = obj.toConfig.getOptionalBoolean("withFormat").getOrElse(false)

        if (!withFormat) {
          val fromTable = obj.toConfig.getOptionalString("in-table").map(_.split(":")).getOrElse(Array("", ""))
          val toTable = obj.toConfig.getOptionalString("out-table").map(_.split(":")).getOrElse(Array("", ""))

          (NumericColumn(fromTable(0), fromTable(1)), NumericColumn(toTable(0), toTable(1)))
        } else {
          val fromTable = obj.toConfig.getOptionalString("in-table").getOrElse("")
          val fromColumns = obj.toConfig.getOptionalStringList("in-columns").map(ColumnParser.parseList).getOrElse(Nil)
          val toTable = obj.toConfig.getOptionalString("out-table").getOrElse("")
          val toColumns = obj.toConfig.getOptionalStringList("out-columns").map(ColumnParser.parseList).getOrElse(Nil)

          (NonnumericColumn(fromTable, fromColumns), NonnumericColumn(toTable, toColumns))
        }
      }
    }

    def getCustomTags: List[Tag] = {
      underlying.getOptionalObjectList("tags").getOrElse(List.empty).map { obj =>
        val key = obj.toConfig.getString("key")
        val value = obj.toConfig.getString("value")

        Tag(key, value)
      }
    }
  }
}
