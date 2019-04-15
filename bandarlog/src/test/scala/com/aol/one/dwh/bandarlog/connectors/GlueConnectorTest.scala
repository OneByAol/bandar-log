/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors
import com.aol.one.dwh.infra.config.{DatetimeColumn, GlueConfig, NumericColumn, Partition}
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

class GlueConnectorTest extends FunSuite with MockitoSugar {

  private val config = GlueConfig("eu-central-1", "default", "accessKey", "secretKey", 5, 2, 10.seconds)
  private val glueConnector = mock[GlueConnector]

  test("Check max batchId from glue metadata tables") {
    val resultValue = 100L
    val numericTable = NumericColumn("table", "column")
    when(glueConnector.getMaxPartitionValue(numericTable)).thenReturn(resultValue)

    val result = glueConnector.getMaxPartitionValue(numericTable)
    assert(result == resultValue)
  }

  test("Check max date partitions' value from glue metadata table") {
    val resultValue = 15681377656L
    val datetimeTable = DatetimeColumn("table", List(Partition("year", "yyyy"), Partition("month", "MM"), Partition("day", "dd")))
    when(glueConnector.getMaxPartitionValue(datetimeTable)).thenReturn(resultValue)

    val result = glueConnector.getMaxPartitionValue(datetimeTable)
    assert(result == resultValue)
  }
}
