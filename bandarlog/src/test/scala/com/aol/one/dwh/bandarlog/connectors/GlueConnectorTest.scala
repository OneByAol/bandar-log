package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.config.GlueConfig
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class GlueConnectorTest extends FunSuite with MockitoSugar {

  private val config = mock[GlueConfig]
  private val glueConnector = mock[GlueConnector]

  test("Check max batchId from glue metadata tables") {
    val resultValue = 100L

    when(glueConnector.getMaxBatchId("table", "column")).thenReturn(resultValue)

    val result = glueConnector.getMaxBatchId("table", "column")
    assert(result == resultValue)
  }
}
