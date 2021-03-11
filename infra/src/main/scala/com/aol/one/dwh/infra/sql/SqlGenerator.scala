package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{Filter, Table}

import java.time.{Duration, Instant}

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
      s"$baseSql WHERE ${filters.map(filter => s"${filter.key} ${getCondition(filter)} ${getValue(filter)}").mkString(" AND ")}"
    }.getOrElse(baseSql)
  }

  private def getCondition(filter: Filter): String = filter.condition match {
    case "lt" => "<"
    case "lte" => "<="
    case "gt" => ">"
    case "gte" => ">="
    case "eq" => "="
    case _ => throw new RuntimeException("Unknown condition " + filter.condition)
  }

  private def getValue(filter: Filter): String = {
    if (filter.dynamic) buildDynamicValue(filter) else buildStaticValue(filter)
  }

  private def buildDynamicValue(filter: Filter): String = {
    val unixMsPattern = "(timestamp_unix_ms:)(.*)".r
    filter.value match {
      case unixMsPattern(_, value) => {
        val timestamp = Instant.now().minus(Duration.parse(s"PT$value"))
        timestamp.toEpochMilli.toString
      }
      case _ => throw new RuntimeException("Unknown dynamic filter pattern: " + filter.value)
    }
  }

  private def buildStaticValue(filter: Filter): String = {
    if (filter.quoted) s"'${filter.value}'" else filter.value
  }
}
