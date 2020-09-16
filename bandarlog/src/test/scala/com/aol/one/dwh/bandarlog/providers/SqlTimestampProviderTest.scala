/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.JdbcConnector
import com.aol.one.dwh.infra.config.Table
import com.aol.one.dwh.infra.sql.{Query, VerticaMaxValuesQuery}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class SqlTimestampProviderTest extends FunSuite with MockitoSugar {

  private val query = VerticaMaxValuesQuery(Table("table", List("column"), filters = None, formats = None, tag = None))
  private val jdbcConnector = mock[JdbcConnector]
  private val sqlTimestampProvider = new SqlTimestampProvider(jdbcConnector, query)

  test("check timestamp value by connector and query") {
    val resultTimestamp = Some(1234567890L)
    when(jdbcConnector.runQuery(any(classOf[Query]), any())).thenReturn(resultTimestamp)

    val result = sqlTimestampProvider.provide()

    assert(result.getValue == resultTimestamp)
  }

  test("return none if can't get timestamp value") {
    when(jdbcConnector.runQuery(any(classOf[Query]), any())).thenReturn(None)

    val result = sqlTimestampProvider.provide()

    assert(result.getValue.isEmpty)
  }
}
