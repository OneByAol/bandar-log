/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import java.sql.{Connection, DatabaseMetaData, ResultSet, Statement}
import com.aol.one.dwh.infra.config._
import com.aol.one.dwh.infra.sql.pool.HikariConnectionPool
import com.aol.one.dwh.infra.sql.{ListStringResultHandler, Setting, VerticaMaxValuesQuery}
import org.apache.commons.dbutils.ResultSetHandler
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyString
import org.mockito.Mockito.{verify, when}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar

class JdbcConnectorTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  private var statement: Statement = _
  private var resultSet: ResultSet = _
  private var connectionPool: HikariConnectionPool = _
  private var connection: Connection = _
  private var databaseMetaData: DatabaseMetaData = _
  private var resultSetHandler: ResultSetHandler[Long] = _
  private var listStringResultHandler: ListStringResultHandler = _

  before {
    statement = mock[Statement]
    resultSet = mock[ResultSet]
    connectionPool = mock[HikariConnectionPool]
    connection = mock[Connection]
    databaseMetaData = mock[DatabaseMetaData]
    resultSetHandler = mock[ResultSetHandler[Long]]
    listStringResultHandler = mock[ListStringResultHandler]
  }

  test("check run query result for numeric batch_id column") {
    val resultValue = 100L
    val table = Table("table", List("column"), filters = None, formats = None, tag = None)
    val query = VerticaMaxValuesQuery(table)
    when(connectionPool.getConnection).thenReturn(connection)
    when(connectionPool.getName).thenReturn("connection_pool_name")
    when(connection.createStatement()).thenReturn(statement)
    when(statement.executeQuery("SELECT MAX(column) AS column FROM table")).thenReturn(resultSet)
    when(connection.getMetaData).thenReturn(databaseMetaData)
    when(databaseMetaData.getURL).thenReturn("connection_url")
    when(resultSetHandler.handle(resultSet)).thenReturn(resultValue)

    val result = new DefaultJdbcConnector(connectionPool).runQuery(query, resultSetHandler)

    assert(result == resultValue)
  }

  test("check run query result for numeric batch_id column with static filter") {
    val resultValue = 100L
    val filters = List(
      Filter("string_col", "value", "eq", quoted = true, dynamic = false),
      Filter("int_col", "1", "eq", quoted = false, dynamic = false)
    )
    val table = Table("table", List("column"), Some(filters), formats = None, tag = None)
    val query = VerticaMaxValuesQuery(table)
    when(connectionPool.getConnection).thenReturn(connection)
    when(connectionPool.getName).thenReturn("connection_pool_name")
    when(connection.createStatement()).thenReturn(statement)
    when(statement.executeQuery("SELECT MAX(column) AS column FROM table WHERE string_col = 'value' AND int_col = 1"))
      .thenReturn(resultSet)
    when(connection.getMetaData).thenReturn(databaseMetaData)
    when(databaseMetaData.getURL).thenReturn("connection_url")
    when(resultSetHandler.handle(resultSet)).thenReturn(resultValue)

    val result = new DefaultJdbcConnector(connectionPool).runQuery(query, resultSetHandler)

    assert(result == resultValue)
  }

  test("check run query result for numeric batch_id column with dynamic filter") {
    val resultValue = 100L
    val filters = List(Filter("int_col", "timestamp_unix_ms:1H", "gte", quoted = false, dynamic = true))
    val table = Table("table", List("column"), Some(filters), formats = None, tag = None)
    val query = VerticaMaxValuesQuery(table)
    when(connectionPool.getConnection).thenReturn(connection)
    when(connectionPool.getName).thenReturn("connection_pool_name")
    when(connection.createStatement()).thenReturn(statement)
    when(statement.executeQuery(anyString())).thenReturn(resultSet)
    when(connection.getMetaData).thenReturn(databaseMetaData)
    when(databaseMetaData.getURL).thenReturn("connection_url")
    when(resultSetHandler.handle(resultSet)).thenReturn(resultValue)

    val result = new DefaultJdbcConnector(connectionPool).runQuery(query, resultSetHandler)

    assert(result == resultValue)
    val sqlCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(statement).executeQuery(sqlCaptor.capture())
    assert(sqlCaptor.getValue.matches("(SELECT MAX\\(column\\) AS column FROM table WHERE int_col >= )(\\d{13})"))
  }

  test("check run query result for date/time partitions") {
    val resultValue = Some(20190924L)
    val table = Table("table", List("year", "month", "day"), filters = None, Some(List("yyyy", "MM", "dd")), tag = None)
    val query = VerticaMaxValuesQuery(table)
    when(connectionPool.getConnection).thenReturn(connection)
    when(connectionPool.getName).thenReturn("connection_pool_name")
    when(connection.createStatement()).thenReturn(statement)
    when(statement.executeQuery("SELECT DISTINCT year, month, day FROM table")).thenReturn(resultSet)
    when(connection.getMetaData).thenReturn(databaseMetaData)
    when(databaseMetaData.getURL).thenReturn("connection_url")
    when(listStringResultHandler.handle(resultSet)).thenReturn(resultValue)

    val result = new DefaultJdbcConnector(connectionPool).runQuery(query, listStringResultHandler)

    assert(result == resultValue)
  }
}

class DefaultJdbcConnector(connectionPool: HikariConnectionPool) extends JdbcConnector(connectionPool) {
  override def applySetting(connection: Connection, statement: Statement, setting: Setting): Unit = {}
}
