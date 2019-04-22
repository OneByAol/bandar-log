package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{DatetimeColumn, NumericColumn, Table}
import org.apache.commons.dbutils.ResultSetHandler

object ResultHandler {

  def get(table: Table): ResultSetHandler[Option[Long]] =
    table match {
      case _: NumericColumn => new LongValueResultHandler

      case datetimeTable: DatetimeColumn =>
        val columns = datetimeTable.columns
        val numberOfColumns = columns.length
        val format = columns.map(_.columnFormat).mkString(":")
        new ListStringResultHandler(numberOfColumns, format)
  }

}