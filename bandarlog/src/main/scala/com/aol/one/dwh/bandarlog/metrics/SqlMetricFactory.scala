/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.connectors.JdbcConnector
import com.aol.one.dwh.infra.sql.MaxValuesQuery
import com.aol.one.dwh.infra.sql.pool.ConnectionPoolHolder
import BaseMetrics.{IN, LAG, OUT}
import Metrics.REALTIME_LAG
import com.aol.one.dwh.bandarlog.providers.{CurrentTimestampProvider, SqlLagProvider, SqlTimestampProvider}
import com.aol.one.dwh.infra.config.{ConnectorConfig, TableColumn, Tag}

/**
  * Sql Metric Factory
  *
  * Provides pair Metric -> Provider by metric id
  */
class SqlMetricFactory(connectionPoolHolder: ConnectionPoolHolder) {

  def create(
              metricId: String,
              metricPrefix: String,
              inConnector: ConnectorConfig,
              outConnectors: Seq[ConnectorConfig],
              inTable: TableColumn,
              outTable: TableColumn
            ): Seq[MetricProvider[Long]] = metricId match {

    case IN =>
      val tags = List(Tag("in_table", inTable.table), Tag("in_connector", inConnector.tag))
      val query = MaxValuesQuery.get(inConnector.connectorType)(inTable)
      val connectionPool = connectionPoolHolder.get(inConnector)

      val inMetric = AtomicMetric[Long](metricPrefix, "in_timestamp", tags)
      val inProvider = new SqlTimestampProvider(JdbcConnector(inConnector.connectorType, connectionPool), query)

      Seq(MetricProvider(inMetric, inProvider))

    case OUT =>
      outConnectors.map { outConnector =>
        val tags = List(Tag("out_table", outTable.table), Tag("out_connector", outConnector.tag))
        val query = MaxValuesQuery.get(outConnector.connectorType)(outTable)
        val connectionPool = connectionPoolHolder.get(outConnector)

        val outMetric = AtomicMetric[Long](metricPrefix, "out_timestamp", tags)
        val outProvider = new SqlTimestampProvider(JdbcConnector(outConnector.connectorType, connectionPool), query)

        MetricProvider(outMetric, outProvider)
      }

    case LAG =>
      val inMetricProvider = create(IN, metricPrefix, inConnector, outConnectors, inTable, outTable).head
      val outMetricProviders = create(OUT, metricPrefix, inConnector, outConnectors, inTable, outTable)

      outMetricProviders.map { outMetricProvider =>
        val tags = inMetricProvider.metric.tags ++ outMetricProvider.metric.tags
        val lagMetric = AtomicMetric[Long](metricPrefix, "lag", tags)
        val lagProvider = new SqlLagProvider(inMetricProvider.provider, outMetricProvider.provider)

        MetricProvider(lagMetric, lagProvider)
      }

    case REALTIME_LAG =>
      val outMetricProviders = create(OUT, metricPrefix, inConnector, outConnectors, inTable, outTable)

      outMetricProviders.map { outMetricProvider =>
        val realtimeLagMetric = AtomicMetric[Long](metricPrefix, "realtime_lag", outMetricProvider.metric.tags)
        val currentTimestampProvider = new CurrentTimestampProvider()
        val realtimeLagProvider = new SqlLagProvider(currentTimestampProvider, outMetricProvider.provider)

        MetricProvider(realtimeLagMetric, realtimeLagProvider)
      }

    case _ =>
      throw new IllegalArgumentException(s"Unsupported sql metric:[$metricId]")
  }
}