/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import java.sql.{Connection, Statement}

import com.aol.one.dwh.infra.sql.Setting
import com.aol.one.dwh.infra.sql.pool.HikariConnectionPool
import com.aol.one.dwh.infra.util.LogTrait
import com.aol.one.dwh.infra.sql.Query
import com.aol.one.dwh.infra.sql.pool.SqlSource._
import com.facebook.presto.jdbc.PrestoConnection
import com.google.common.cache.CacheBuilder
import com.mysql.cj.jdbc.JdbcConnection
import com.vertica.jdbc.VerticaConnection
import org.apache.commons.dbutils.ResultSetHandler
import resource.managed

import scala.concurrent.duration._
import scala.util.Try
import scalacache.guava.GuavaCache
import scalacache.memoization._
import scalacache.{CacheConfig, ScalaCache}


abstract class JdbcConnector(@cacheKeyExclude pool: HikariConnectionPool) extends LogTrait {

  implicit val scalaCache = ScalaCache(
    GuavaCache(CacheBuilder.newBuilder().maximumSize(100).build[String, Object]),
    cacheConfig = CacheConfig(keyPrefix = Some(pool.getName))
  )

  def runQuery[V](query: Query, @cacheKeyExclude handler: ResultSetHandler[V]): V = memoizeSync(50.seconds) {
    val rm =
      for {
        connection <- managed(pool.getConnection)
        statement  <- managed(connection.createStatement())
      } yield {
        applySettings(connection, statement, query.settings)
        logger.info(s"Running query:[${query.sql}] source:[${query.source}] settings:[${query.settings.mkString(",")}]")
        val resultSet = statement.executeQuery(query.sql)
        handler.handle(resultSet)
      }

    Try(rm.acquireAndGet(identity)).getOrElse(throw new RuntimeException(s"Failure:[$query]"))
  }

  private def applySettings(connection: Connection, statement: Statement, settings: Seq[Setting]) = {
    settings.foreach(setting => applySetting(connection, statement, setting))
  }

  def applySetting(connection: Connection, statement: Statement, setting: Setting)

}

object JdbcConnector {

  private class PrestoConnector(connectionPool: HikariConnectionPool) extends JdbcConnector(connectionPool) {
    override def applySetting(connection: Connection, statement: Statement, setting: Setting): Unit = {
      connection.unwrap(classOf[PrestoConnection]).setSessionProperty(setting.key, setting.value)
    }
  }

  private class VerticaConnector(connectionPool: HikariConnectionPool) extends JdbcConnector(connectionPool) {
    override def applySetting(connection: Connection, statement: Statement, setting: Setting): Unit = {
      connection.unwrap(classOf[VerticaConnection]).setProperty(setting.key, setting.value)
    }
  }

  private class MySQLConnector(connectionPool: HikariConnectionPool) extends JdbcConnector(connectionPool) {
    override def applySetting(connection: Connection, statement: Statement, setting: Setting): Unit = {
      connection.unwrap(classOf[JdbcConnection]).setClientInfo(setting.key, setting.value)
    }
  }

  def apply(connectorType: String, connectionPool: HikariConnectionPool): JdbcConnector = connectorType match {
    case VERTICA => new VerticaConnector(connectionPool)
    case PRESTO => new PrestoConnector(connectionPool)
    case MYSQL => new MySQLConnector(connectionPool)
    case _ => throw new IllegalArgumentException(s"Can't create connector for SQL source:[$connectorType]")
  }
}
