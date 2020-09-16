/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.metrics.BaseMetrics._
import com.aol.one.dwh.bandarlog.metrics.SqlMetricFactoryTest._
import com.aol.one.dwh.bandarlog.metrics.Metrics.REALTIME_LAG
import com.aol.one.dwh.bandarlog.providers.{ProviderFactory, SqlLagProvider, SqlTimestampProvider}
import com.aol.one.dwh.infra.config._
import com.aol.one.dwh.infra.sql.pool.{ConnectionPoolHolder, HikariConnectionPool}
import com.typesafe.config.Config
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

object SqlMetricFactoryTest {
  private val metricPrefix = "sql_prefix"
  private val inTable = Table("in_test_table", List("in_test_column"), filters = None, formats = None, tag = None)
  private val outTable = Table("out_test_table", List("out_test_column"), filters = None, formats = None, tag = None)
}

class SqlMetricFactoryTest extends FunSuite with MockitoSugar {
  private val connectorPoolHolder = mock[ConnectionPoolHolder]
  private val connectionPool = mock[HikariConnectionPool]
  private val mainConfig = mock[Config]
  private val providerFactory = new ProviderFactory(mainConfig, connectorPoolHolder)
  private val metricFactory = new MetricFactory(providerFactory)

  test("create sql Metric & Provider for IN metric id") {
    mockConnectionPool()
    val inConnector = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(IN, metricPrefix, inConnector, Seq.empty, inTable, outTable)

    assert(results.size == 1)
    assertMetric(results.head.metric, "in_timestamp", List(Tag("in_table", "in_test_table"), Tag("in_connector", "test-vertica")))
    assert(results.head.provider.isInstanceOf[SqlTimestampProvider])
  }

  test("create sql Metric & Provider for IN metric id with alternative table tag") {
    mockConnectionPool()
    val inConnector = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(IN, metricPrefix, inConnector, Nil, inTable.copy(tag = Some("alt_in_tag")), outTable.copy(tag = Some("alt_out_tag")))

    assert(results.size == 1)
    assertMetric(results.head.metric, "in_timestamp", List(Tag("in_table", "alt_in_tag"), Tag("in_connector", "test-vertica")))
    assert(results.head.provider.isInstanceOf[SqlTimestampProvider])
  }

  test("create sql Metric & Provider for OUT metric id") {
    mockConnectionPool()
    val outConnector1 = ConnectorConfig("presto", "presto_config_id", "test-presto")
    val outConnector2 = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(OUT, metricPrefix, None.orNull, Seq(outConnector1, outConnector2), inTable, outTable)

    assert(results.size == 2)

    val metricProvider1 = results.head
    assertMetric(metricProvider1.metric, "out_timestamp", List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-presto")))
    assert(metricProvider1.provider.isInstanceOf[SqlTimestampProvider])

    val metricProvider2 = results(1)
    assertMetric(metricProvider2.metric, "out_timestamp", List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-vertica")))
    assert(metricProvider2.provider.isInstanceOf[SqlTimestampProvider])
  }

  test("create sql Metric & Provider for OUT metric id with alternative table tag") {
    mockConnectionPool()
    val outConnector1 = ConnectorConfig("presto", "presto_config_id", "test-presto")
    val outConnector2 = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(OUT, metricPrefix, None.orNull, Seq(outConnector1, outConnector2),
      inTable.copy(tag = Some("alt_in_tag")), outTable.copy(tag = Some("alt_out_tag")))

    assert(results.size == 2)

    val metricProvider1 = results.head
    assertMetric(metricProvider1.metric, "out_timestamp", List(Tag("out_table", "alt_out_tag"), Tag("out_connector", "test-presto")))
    assert(metricProvider1.provider.isInstanceOf[SqlTimestampProvider])

    val metricProvider2 = results(1)
    assertMetric(metricProvider2.metric, "out_timestamp", List(Tag("out_table", "alt_out_tag"), Tag("out_connector", "test-vertica")))
    assert(metricProvider2.provider.isInstanceOf[SqlTimestampProvider])
  }

  test("create sql Metric & Provider for LAG metric id") {
    mockConnectionPool()
    val inConnector = ConnectorConfig("presto", "presto_config_id_1", "in_test-presto")
    val outConnector1 = ConnectorConfig("presto", "presto_config_id_2", "out_test-presto")
    val outConnector2 = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(LAG, metricPrefix, inConnector, Seq(outConnector1, outConnector2), inTable, outTable)

    assert(results.size == 2)

    val metricProvider1 = results.head
    val tags1 = List(
      Tag("in_table", "in_test_table"),
      Tag("in_connector", "in_test-presto"),
      Tag("out_table", "out_test_table"),
      Tag("out_connector", "out_test-presto")
    )
    assertMetric(metricProvider1.metric, "lag", tags1)
    assert(metricProvider1.provider.isInstanceOf[SqlLagProvider])

    val metricProvider2 = results(1)
    val tags2 = List(
      Tag("in_table", "in_test_table"),
      Tag("in_connector", "in_test-presto"),
      Tag("out_table", "out_test_table"),
      Tag("out_connector", "test-vertica")
    )
    assertMetric(metricProvider2.metric, "lag", tags2)
    assert(metricProvider2.provider.isInstanceOf[SqlLagProvider])
  }

  test("create sql Metric & Provider for REALTIME_LAG metric id") {
    mockConnectionPool()
    val outConnector1 = ConnectorConfig("presto", "presto_config_id_2", "out_test-presto")
    val outConnector2 = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(REALTIME_LAG, metricPrefix, None.orNull, Seq(outConnector1, outConnector2), inTable, outTable)

    assert(results.size == 2)

    val metricProvider1 = results.head
    val tags1 = List(Tag("out_table", "out_test_table"), Tag("out_connector", "out_test-presto"))
    assertMetric(metricProvider1.metric, "realtime_lag", tags1)
    assert(metricProvider1.provider.isInstanceOf[SqlLagProvider])

    val metricProvider2 = results(1)
    val tags2 = List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-vertica"))
    assertMetric(metricProvider2.metric, "realtime_lag", tags2)
    assert(metricProvider2.provider.isInstanceOf[SqlLagProvider])
  }

  test("throw exception in unknown metric case") {
    intercept[IllegalArgumentException] {
      metricFactory.create("UNKNOWN_METRIC", metricPrefix, None.orNull, Seq.empty, inTable, outTable)
    }
  }

  private def mockConnectionPool() = {
    when(connectorPoolHolder.get(any())).thenReturn(connectionPool)
    when(connectionPool.getName).thenReturn("connection_pool_name")
  }

  private def assertMetric[V](metric: Metric[V], expectedName: String, expectedTags: List[Tag]) = {
    assert(metric.prefix == metricPrefix)
    assert(metric.name == expectedName)
    assert(metric.tags == expectedTags)
    assert(metric.value == AtomicValue(None))
  }
}
