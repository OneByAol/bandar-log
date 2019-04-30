package com.aol.one.dwh.infra.sql

import org.apache.commons.dbutils.ResultSetHandler

object QueryResulthandler {

  def get(query: Query): ResultSetHandler[Option[Long]] = query match {
      case VerticaMaxValuesQuery(table) =>
        if (table.formats.isDefined) {
          val parititions = table.columns
          val numberOfColumns = parititions.length
          val format = table.formats.get.mkString(":")
          new ListStringResultHandler(numberOfColumns, format)
        } else {
          new LongValueResultHandler
        }

      case PrestoMaxValuesQuery(table) =>
        if (table.formats.isDefined) {
          val parititions = table.columns
          val numberOfColumns = parititions.length
          val format = table.formats.get.mkString(":")
          new ListStringResultHandler(numberOfColumns, format)
        } else {
          new LongValueResultHandler
        }
    }
}
