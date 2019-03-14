/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{Partition, TableColumn, TablePartition}
import com.aol.one.dwh.infra.sql.pool.SqlSource._

import scala.annotation.tailrec

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

case class VerticaMaxValuesQuery(tableColumn: TableColumn) extends VerticaQuery {
  override def sql: String = s"SELECT MAX(${tableColumn.column}) AS ${tableColumn.column} FROM ${tableColumn.table}"

  override def settings: Seq[Setting] = Seq.empty
}

case class PrestoMaxValuesQuery(tableColumn: TableColumn) extends PrestoQuery {
  override def sql: String = s"SELECT MAX(${tableColumn.column}) AS ${tableColumn.column} FROM ${tableColumn.table}"

  override def settings: Seq[Setting] = Seq(Setting("optimize_metadata_queries", "true"))
}

case class  PrestoMaxWhereValuesQuery(partition: Partition, table: String, partitionSpec: (String, String)) extends PrestoQuery {
  val column = partition.column
  val (partitionName, maxValue) = partitionSpec
  override def sql: String = s"select max($column) from $table where $partitionName='$maxValue'"

  override def settings: Seq[Setting] = Seq(Setting("optimize_metadata_queries", "true"))
}

object MaxValuesQuery {

  def get(source: String): TableColumn => Query = source match {
    case PRESTO => PrestoMaxValuesQuery
    case VERTICA => VerticaMaxValuesQuery
    case s => throw new IllegalArgumentException(s"Can't get query for source:[$s]")
  }
}
