/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.GlueConnector
import com.aol.one.dwh.infra.config.TableColumn
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class GlueTimestampProviderTest extends FunSuite with MockitoSugar{

  private val table = mock[TableColumn]
  private val glueConnector = mock[GlueConnector]
  private val glueTimestampProvider = new GlueTimestampProvider(glueConnector, table)

  test("check timestamp value by glue connector and table") {
    val glueTimestamp = 1533709910004L
    when(glueConnector.getMaxPartitionValue(any())).thenReturn(glueTimestamp)

    val result = glueTimestampProvider.provide()

    assert(result.getValue == Some(glueTimestamp))
  }

  test("return zero if partition column does not have values") {
    when(glueConnector.getMaxPartitionValue(any())).thenReturn(0)

    val result = glueTimestampProvider.provide()

    assert(result.getValue == Some(0))
  }
}
