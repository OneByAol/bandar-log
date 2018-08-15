/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.{GlueConnector, JdbcConnector}
import com.aol.one.dwh.infra.config.TableColumn
import com.aol.one.dwh.infra.sql.Query
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class SqlTimestampProviderTest extends FunSuite with MockitoSugar {

  private val query = mock[Query]
  private val table = mock[TableColumn]
  private val jdbcConnector = mock[JdbcConnector]
  private val glueConnector = mock[GlueConnector]
  private val sqlTimestampProvider = new SqlTimestampProvider(jdbcConnector, query)
  private val glueTimestampProvider = new GlueTimestampProvider(glueConnector, table)

  test("check timestamp value by connector and query") {
    val resultTimestamp = Some(1234567890L)
    when(jdbcConnector.runQuery(any(classOf[Query]), any())).thenReturn(resultTimestamp)

    val result = sqlTimestampProvider.provide()

    assert(result.getValue == resultTimestamp)
  }

  test("check timestamp value by glue connector and table") {
    val glueTimestamp = 1533709910004L
    when(glueConnector.getMaxBatchId(any(), any())).thenReturn(glueTimestamp)

    val result = glueTimestampProvider.provide()

    assert(result.getValue == Some(glueTimestamp))
  }

  test("return none if can't get timestamp value") {
    when(jdbcConnector.runQuery(any(classOf[Query]), any())).thenReturn(None)

    val result = sqlTimestampProvider.provide()

    assert(result.getValue.isEmpty)
  }

  test("return zero if can't get timestamp value in glue connector") {
    when(glueConnector.getMaxBatchId(any(), any())).thenReturn(0)

    val result = glueTimestampProvider.provide()

    assert(result.getValue == Some(0))
  }

}
