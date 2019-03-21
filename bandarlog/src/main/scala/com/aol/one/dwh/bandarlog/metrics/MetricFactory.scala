/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.metrics.BaseMetrics.{IN, LAG, OUT}
import com.aol.one.dwh.bandarlog.metrics.Metrics.REALTIME_LAG
import com.aol.one.dwh.bandarlog.providers._
import com.aol.one.dwh.infra.config.{ConnectorConfig, Table, Tag}

class MetricFactory(provider: ProviderFactory) {

  def create(
    metricId: String,
    metricPrefix: String,
    inConnector: ConnectorConfig,
    outConnectors: Seq[ConnectorConfig],
    inTable: Table,
    outTable: Table
  ): Seq[MetricProvider[Long]] = metricId match {

    case IN =>
      val tags = List(Tag("in_table", inTable.tableName), Tag("in_connector", inConnector.tag))
      val inMetric = AtomicMetric[Long](metricPrefix, "in_timestamp", tags)
      val inProvider = provider.create(inConnector, inTable)
      Seq(MetricProvider(inMetric, inProvider))

    case OUT =>
      outConnectors.map { outConnector =>
        val tags = List(Tag("out_table", outTable.tableName), Tag("out_connector", outConnector.tag))
        val outMetric = AtomicMetric[Long](metricPrefix, "out_timestamp", tags)
        val outProvider = provider.create(outConnector, outTable)
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
