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
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class JdbcConnectorTest extends FunSuite with MockitoSugar {

  private val statement = mock[Statement]
  private val resultSet = mock[ResultSet]
  private val connectionPool = mock[HikariConnectionPool]
  private val connection = mock[Connection]
  private val databaseMetaData = mock[DatabaseMetaData]
  private val resultSetHandler = mock[ResultSetHandler[Long]]
  private val listStringResultHandler = mock[ListStringResultHandler]

  test("check run query result for numeric batch_id column") {
    val resultValue = 100L
    val table = Table("table", List("column"), None)
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

  test("check run query result for date/time partitions") {
    val resultValue = Some(20190924L)
    val table = Table("table", List("year", "month", "day"), Some(List("yyyy", "MM", "dd")))
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
