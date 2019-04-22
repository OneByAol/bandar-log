/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{DatetimeColumn, NumericColumn, Table}
import com.aol.one.dwh.infra.sql.pool.SqlSource._

/**
  * Base Query interface
  */
trait Query {
  def sql: String
  def settings: Seq[Setting]
  def source: String
}

/**
  * Presto query marker
  */
trait PrestoQuery extends Query {
  override def source: String = PRESTO
}

/**
  * Vertica query marker
  */
trait VerticaQuery extends Query {
  override def source: String = VERTICA
}

case class VerticaNumericValuesQuery(table: NumericColumn) extends VerticaQuery {
  override def sql: String =  s"SELECT MAX(${table.column}) AS ${table.column} FROM ${table.tableName}"

  override def settings: Seq[Setting] = Seq.empty

}

case class VerticaDatetimeValuesQuery(table: DatetimeColumn) extends VerticaQuery {
  val columns = table.columns.map(_.columnName)

  override def sql: String = s"SELECT DISTINCT ${columns.mkString(", ")} FROM ${table.tableName}"

  override def settings: Seq[Setting] = Seq.empty

}

case class PrestoNumericValuesQuery(table: NumericColumn) extends PrestoQuery {
  override def sql: String =  s"SELECT MAX(${table.column}) AS ${table.column} FROM ${table.tableName}"

  override def settings: Seq[Setting] = Seq(Setting("optimize_metadata_queries", "true"))

}

case class PrestoDatetimeValuesQuery(table: DatetimeColumn) extends PrestoQuery {
  val columns = table.columns.map(_.columnName)

  override def sql: String = s"SELECT DISTINCT ${columns.mkString(", ")} FROM ${table.tableName}"

  override def settings: Seq[Setting] = Seq(Setting("optimize_metadata_queries", "true"))

}

object ValuesQuery {
  import com.aol.one.dwh.infra.sql.query.GenericQuery
  import com.aol.one.dwh.infra.sql.query.QueryMaker._

  def get(source: String, table: Table): Query = table match {
    case table: NumericColumn  => implicitly[GenericQuery[NumericColumn]].getQuery(source, table)
    case table: DatetimeColumn => implicitly[GenericQuery[DatetimeColumn]].getQuery(source, table)
  }
}
