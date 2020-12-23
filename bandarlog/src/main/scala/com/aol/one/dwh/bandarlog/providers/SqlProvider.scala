/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import java.time.Instant

import com.aol.one.dwh.bandarlog.connectors.{GlueConnector, JdbcConnector}
import com.aol.one.dwh.bandarlog.metrics.{AtomicValue, Value}
import com.aol.one.dwh.bandarlog.providers.CurrentTimestampProvider.{MINUTES_IN_HOUR, SECONDS_IN_MINUTE}
import com.aol.one.dwh.bandarlog.providers.SqlProvider._
import com.aol.one.dwh.infra.config.Table
import com.aol.one.dwh.infra.sql.{QueryResultHandler, _}
import com.aol.one.dwh.infra.util.LogTrait

object SqlProvider {
  type Timestamp = Long
  type TimestampProvider = Provider[Timestamp]
}

/**
  * Sql Timestamp Provider
  *
  * Provides timestamp metric by query and appropriate connector
  */
class SqlTimestampProvider(connector: JdbcConnector, query: Query) extends TimestampProvider with LogTrait {

  override def provide(): Value[Timestamp] = {
    AtomicValue(connector.runQuery(query, QueryResultHandler.get(query)))
  }
}

/**
  * Glue Timestamp Provider
  *
  * Provides timestamp metric by table, partition column and appropriate connector
  */
class GlueTimestampProvider(connector: GlueConnector, table: Table) extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    AtomicValue(Option(connector.getMaxPartitionValue(table)))
  }
}

/**
  * Current Timestamp Provider
  *
  * Provides current time in epoch milliseconds, seconds, minutes or hours, based on a timestamp type provided
  */
class CurrentTimestampProvider(timestampType: Option[String]) extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    def now(): Long = {
      val currentTime = Instant.now()
      timestampType match {
        case Some("hour") => currentTime.getEpochSecond / SECONDS_IN_MINUTE / MINUTES_IN_HOUR // convert to epoch hours
        case Some("minute") => currentTime.getEpochSecond / SECONDS_IN_MINUTE                 // convert to epoch minutes
        case Some("second") => currentTime.getEpochSecond
        case _ => currentTime.toEpochMilli
      }
    }

    AtomicValue(Option(now()))
  }
}

object CurrentTimestampProvider {
  val SECONDS_IN_MINUTE, MINUTES_IN_HOUR = 60
}

/**
  * Sql Lag Provider
  *
  * Provides diff between metrics from "fromProvider" and "toProvider"
  *
  * Lag (diff) = fromProvider.metric - toProvider.metric
  */
class SqlLagProvider(fromProvider: TimestampProvider, toProvider: TimestampProvider) extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    val lag =
      for {
        fromValue <- fromProvider.provide().getValue
        toValue   <- toProvider.provide().getValue
        lagValue  <- Option(fromValue - toValue)
      } yield lagValue

    AtomicValue(lag)
  }
}
