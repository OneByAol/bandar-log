package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.Table

object SqlGenerator {

  def generate(table: Table): String = {
    val baseSql = table.formats match {
      case Some(_) =>
        val columns = table.columns.mkString(", ")
        s"SELECT DISTINCT $columns FROM ${table.table}"
      case None =>
        val column = table.columns.head
        s"SELECT MAX($column) AS $column FROM ${table.table}"
    }

    table.filters.map { filters =>
      s"$baseSql WHERE ${filters.map { case (column, value) => s"$column = $value" }.mkString(" AND ")}"
    }.getOrElse(baseSql)
  }
}
