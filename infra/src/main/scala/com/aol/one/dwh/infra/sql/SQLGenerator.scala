package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.TableColumn

object SQLGenerator {

  def generateSql(table: TableColumn): String = {
    if (table.formats.isDefined) {
      val columns = table.columns.mkString(", ")
      s"SELECT DISTINCT $columns FROM ${table.table}"
    } else {
      val column = table.columns.head
      s"SELECT MAX($column) AS $column FROM ${table.table}"
    }
  }

}
