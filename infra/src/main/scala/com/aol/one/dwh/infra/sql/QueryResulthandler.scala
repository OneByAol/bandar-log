package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.Table
import org.apache.commons.dbutils.ResultSetHandler

object QueryResulthandler {

  def get(query: Query): ResultSetHandler[Option[Long]] = query match {
      case VerticaMaxValuesQuery(table) => getHandler(table)
      case PrestoMaxValuesQuery(table) => getHandler(table)
      case ClickhouseMaxValuesQuery(table) => getHandler(table)
    }

  private def getHandler(table: Table): ResultSetHandler[Option[Long]] = {
    table
      .formats
      .map { format =>
        val parititions = table.columns
        val numberOfColumns = parititions.length
        new ListStringResultHandler(numberOfColumns, format.mkString(":"))
      }
      .getOrElse(new LongValueResultHandler)
  }

}
