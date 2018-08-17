/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.{GlueConnector, JdbcConnector}
import com.aol.one.dwh.bandarlog.metrics.{AtomicValue, Value}
import com.aol.one.dwh.infra.sql.LongValueResultHandler
import com.aol.one.dwh.infra.sql.Query
import SqlProvider._
import com.aol.one.dwh.infra.config.TableColumn

object SqlProvider {
  type Timestamp = Long
  type TimestampProvider = Provider[Timestamp]
}

/**
  * Sql Timestamp Provider
  *
  * Provides timestamp metric by query and appropriate connector
  */
class SqlTimestampProvider(connector: JdbcConnector, query: Query) extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    AtomicValue(connector.runQuery(query, new LongValueResultHandler))
  }
}

/**
  * Glue Timestamp Provider
  *
  * Provides timestamp metric by table, partition column and appropriate connector
  */
class GlueTimestampProvider(connector: GlueConnector, tableInfo: TableColumn) extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    AtomicValue(Option(connector.getMaxBatchId(tableInfo.table, tableInfo.column)))
  }
}

/**
  * Current Timestamp Provider
  *
  * Provides current time in milliseconds
  */
class CurrentTimestampProvider extends TimestampProvider {

  override def provide(): Value[Timestamp] = {
    AtomicValue(Option(System.currentTimeMillis()))
  }
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
