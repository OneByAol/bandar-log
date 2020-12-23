/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql.pool

import java.util.Objects

import com.aol.one.dwh.infra.config.RichConfig._
import com.aol.one.dwh.infra.config.{ConnectorConfig, JdbcConfig}
import com.aol.one.dwh.infra.sql.pool.SqlSource._
import com.facebook.presto.jdbc.PrestoDriver
import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig

object SqlSource {
  val VERTICA = "vertica"
  val PRESTO = "presto"
  val GLUE = "glue"
  val CLICKHOUSE = "clickhouse"
}

object PoolConfig {
  def apply(connectorConf: ConnectorConfig, mainConf: Config): HikariConfig = {
    connectorConf.connectorType match {
      case VERTICA => VerticaPoolConfig(mainConf.getJdbcConfig(connectorConf.configId))
      case PRESTO => PrestoPoolConfig(mainConf.getJdbcConfig(connectorConf.configId))
      case CLICKHOUSE => ClickhousePoolConfig(mainConf.getJdbcConfig(connectorConf.configId))
      case _ => throw new IllegalArgumentException(s"Unsupported connector type:[${connectorConf.connectorType}]")
    }
  }
}

private object PrestoPoolConfig {
  def apply(jdbcConfig: JdbcConfig): HikariConfig = {
    val config: HikariConfig = new HikariConfig
    config.setPoolName(s"presto-pool-${jdbcConfig.dbName}")
    config.setDriverClassName(classOf[PrestoDriver].getName)
    config.setJdbcUrl(s"jdbc:presto://${jdbcConfig.host}:${jdbcConfig.port}/hive/${jdbcConfig.dbName}")
    config.setUsername(jdbcConfig.username)
    config.setMaximumPoolSize(jdbcConfig.maxPoolSize)
    config.setConnectionTimeout(jdbcConfig.connectionTimeout)
    config.setReadOnly(true)
    config
  }
}

private object ClickhousePoolConfig {
  def apply(jdbcConfig: JdbcConfig): HikariConfig = {
    val config: HikariConfig = new HikariConfig
    config.setPoolName(s"clickhouse-pool-${jdbcConfig.dbName}")
    config.setDriverClassName(classOf[cc.blynk.clickhouse.ClickHouseDriver].getName)
    config.setJdbcUrl(s"jdbc:clickhouse://${jdbcConfig.host}:${jdbcConfig.port}/${jdbcConfig.dbName}")
    config.setUsername(jdbcConfig.username)
    config.setMaximumPoolSize(jdbcConfig.maxPoolSize)
    config.setConnectionTimeout(jdbcConfig.connectionTimeout)
    config.setReadOnly(true)
    config
  }
}

private object VerticaPoolConfig {
  def apply(jdbcConfig: JdbcConfig): HikariConfig = {

    val verticaUrl = {
      val baseUri = s"jdbc:vertica://${jdbcConfig.host}:${jdbcConfig.port}/${jdbcConfig.dbName}"
      val schema = if (Objects.nonNull(jdbcConfig.schema)) "?connsettings=SET SEARCH_PATH TO " + jdbcConfig.schema else ""
      val ssl = if (jdbcConfig.useSsl) "&ssl=true" else ""

      baseUri + schema + ssl
    }

    val config: HikariConfig = new HikariConfig
    config.setPoolName(s"vertica-pool-${jdbcConfig.dbName}")
    config.setDriverClassName(classOf[com.vertica.jdbc.Driver].getName)
    config.setJdbcUrl(verticaUrl)
    config.setUsername(jdbcConfig.username)
    config.setPassword(jdbcConfig.password)
    config.setMaximumPoolSize(jdbcConfig.maxPoolSize)
    config.setConnectionTimeout(jdbcConfig.connectionTimeout)
    config.setConnectionTestQuery("SELECT 1")
    config.setReadOnly(true)
    config.setAutoCommit(false)
    config
  }
}
