/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{Table, TableColumn, TablePartition}
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

case class VerticaValuesQuery(table: Table) extends VerticaQuery {
  override def sql: String = {
    table match {
      case _: TableColumn => s"SELECT MAX(${table.columns.head}) AS ${table.columns.head} FROM ${table.table}"
      case _: TablePartition => s"SELECT ${table.columns.mkString(", ")} FROM ${table.table}"
    }
  }

  override def settings: Seq[Setting] = Seq.empty
}

case class PrestoValuesQuery(table: Table) extends PrestoQuery {
  override def sql: String = {
    table match {
      case _: TableColumn => s"SELECT MAX(${table.columns.head}) AS ${table.columns.head} FROM ${table.table}"
      case _: TablePartition => s"SELECT ${table.columns.mkString(", ")} FROM ${table.table}"
    }
  }

  override def settings: Seq[Setting] = Seq(Setting("optimize_metadata_queries", "true"))
}

object MaxValuesQuery {

  def get(source: String): Table => Query = source match {
    case PRESTO => PrestoValuesQuery
    case VERTICA => VerticaValuesQuery
    case s => throw new IllegalArgumentException(s"Can't get query for source:[$s]")
  }
}
