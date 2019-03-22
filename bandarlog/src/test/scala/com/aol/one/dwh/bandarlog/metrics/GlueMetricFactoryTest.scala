package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.metrics.BaseMetrics.{IN, LAG, OUT}
import com.aol.one.dwh.bandarlog.metrics.Metrics.REALTIME_LAG
import com.aol.one.dwh.bandarlog.providers.{GlueTimestampProvider, ProviderFactory, SqlLagProvider}
import com.aol.one.dwh.infra.config.{ConnectorConfig, NumericColumn, Tag}
import com.aol.one.dwh.infra.sql.pool.{ConnectionPoolHolder, HikariConnectionPool}
import com.typesafe.config.Config
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

object GlueMetricFactoryTest {
  private val metricPrefix = "sql_prefix"
  private val inTable = NumericColumn("in_test_table", "in_test_column")
  private val outTable = NumericColumn("out_test_table", "out_test_column")
}

class GlueMetricFactoryTest extends FunSuite with MockitoSugar {

  import com.aol.one.dwh.bandarlog.metrics.GlueMetricFactoryTest._

  private val connectorPoolHolder = mock[ConnectionPoolHolder]
  private val connectionPool = mock[HikariConnectionPool]
  private val providerFactory = mock[ProviderFactory]
  private val metricFactory = new MetricFactory(providerFactory)
  private val configsForGlue = mock[Config]
  private val glueProvider = mock[GlueTimestampProvider]

  test("create glue Metric & Provider for IN metric id") {
    mockGlueProvider()
    val inConnector = ConnectorConfig("glue", "glue-config", "test-glue")
    val results = metricFactory.create(IN, metricPrefix, inConnector, Seq.empty, inTable, outTable)

    assert(results.size == 1)
    assertMetric(results.head.metric, "in_timestamp", List(Tag("in_table", "in_test_table"), Tag("in_connector", "test-glue")))
    assert(results.head.provider.isInstanceOf[GlueTimestampProvider])
  }

  test("create glue Metric & Provider for OUT metric id") {
    mockGlueProvider()
    val outConnector1 = ConnectorConfig("glue", "glue-config", "test-glue1")
    val outConnector2 = ConnectorConfig("glue", "glue-config", "test-glue2")
    val results = metricFactory.create(OUT, metricPrefix, None.orNull, Seq(outConnector1, outConnector2), inTable, outTable)

    assert(results.size == 2)

    val metricProvider = results.head
    assertMetric(metricProvider.metric, "out_timestamp", List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-glue1")))
    assert(metricProvider.provider.isInstanceOf[GlueTimestampProvider])

    val metricProvider2 = results(1)
    assertMetric(metricProvider2.metric, "out_timestamp", List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-glue2")))
    assert(metricProvider2.provider.isInstanceOf[GlueTimestampProvider])
  }

  test("create sql/glue Metric & Provider for LAG metric id") {
    mockConnectionPool()
    val inConnector = ConnectorConfig("presto", "presto_config_id", "test-presto")
    val outConnector = ConnectorConfig("glue", "glue_config_id", "test-glue")

    val results = metricFactory.create(LAG, metricPrefix, inConnector, Seq(outConnector), inTable, outTable)

    assert(results.size == 1)

    val metricProvider = results.head
    val tags1 = List(
      Tag("in_table", "in_test_table"),
      Tag("in_connector", "test-presto"),
      Tag("out_table", "out_test_table"),
      Tag("out_connector", "test-glue")
    )
    assertMetric(metricProvider.metric, "lag", tags1)
    assert(metricProvider.provider.isInstanceOf[SqlLagProvider])
  }

  test("create glue Metric & Provider for REALTIME_LAG metric id") {
    mockConnectionPool()
    val outConnector1 = ConnectorConfig("glue", "glue_config_id", "test-glue")

    val results = metricFactory.create(REALTIME_LAG, metricPrefix, None.orNull, Seq(outConnector1), inTable, outTable)

    assert(results.size == 1)

    val metricProvider1 = results.head
    val tags1 = List(Tag("out_table", "out_test_table"), Tag("out_connector", "test-glue"))
    assertMetric(metricProvider1.metric, "realtime_lag", tags1)
    assert(metricProvider1.provider.isInstanceOf[SqlLagProvider])
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

  private def mockGlueProvider() = {
    when(providerFactory.create(any(), any())).thenReturn(glueProvider)
  }

  private def assertMetric[V](metric: Metric[V], expectedName: String, expectedTags: List[Tag]) = {
    assert(metric.prefix == metricPrefix)
    assert(metric.name == expectedName)
    assert(metric.tags == expectedTags)
    assert(metric.value == AtomicValue(None))
  }
}
