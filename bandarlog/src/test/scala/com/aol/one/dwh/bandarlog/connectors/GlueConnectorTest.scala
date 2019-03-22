/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.config.{GlueConfig, NumericColumn}
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class GlueConnectorTest extends FunSuite with MockitoSugar {

  private val config = mock[GlueConfig]
  private val glueConnector = mock[GlueConnector]

  test("Check max batchId from glue metadata tables") {
    val resultValue = 100L
    val table = NumericColumn("table", "column")
    when(glueConnector.getMaxPartitionValue(table)).thenReturn(resultValue)

    val result = glueConnector.getMaxPartitionValue(table)
    assert(result == resultValue)
  }
}
