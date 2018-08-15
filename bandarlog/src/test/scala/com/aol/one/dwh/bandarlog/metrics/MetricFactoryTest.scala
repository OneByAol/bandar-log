package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.connectors.GlueConnector
import com.aol.one.dwh.bandarlog.metrics.BaseMetrics._
import com.aol.one.dwh.bandarlog.metrics.MetricFactoryTest._
import com.aol.one.dwh.bandarlog.metrics.Metrics.REALTIME_LAG
import com.aol.one.dwh.bandarlog.providers.{ProviderFactory, SqlLagProvider, SqlTimestampProvider}
import com.aol.one.dwh.infra.config._
import com.aol.one.dwh.infra.sql.pool.{ConnectionPoolHolder, HikariConnectionPool}
import com.typesafe.config.Config
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

object MetricFactoryTest {
  private val metricPrefix = "sql_prefix"
  private val inTable = TableColumn("in_test_table", "in_test_column")
  private val outTable = TableColumn("out_test_table", "out_test_column")
}

class MetricFactoryTest extends FunSuite with MockitoSugar {
  private val connectorPoolHolder = mock[ConnectionPoolHolder]
  private val connectionPool = mock[HikariConnectionPool]
  private val mainConfig = mock[Config]
  private val providerFactory = new ProviderFactory(mainConfig, connectorPoolHolder)
  private val bandarlogConf = mock[Config]
  private val metricFactory = new MetricFactory(connectorPoolHolder, bandarlogConf, providerFactory)

  test("create sql Metric & Provider for IN metric id") {
    mockConnectionPool()
    val inConnector = ConnectorConfig("vertica", "vertica_config_id", "test-vertica")

    val results = metricFactory.create(IN, metricPrefix, inConnector, Seq.empty, inTable, outTable)

    assert(results.size == 1)
    assertMetric(results.head.metric, "in_timestamp", List(Tag("in_table", "in_test_table"), Tag("in_connector", "test-vertica")))
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

  test("create glue Metric & Provider for OUT metric id") {
    val outConnector = ConnectorConfig("glue", "glue-config", "test-glue")
    //TODO
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
