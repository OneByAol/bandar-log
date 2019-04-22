package com.aol.one.dwh.infra.sql

import com.aol.one.dwh.infra.config.{DatetimeColumn, NumericColumn, Table}
import org.apache.commons.dbutils.ResultSetHandler

object ResultHandler {

  def get(query: Query): ResultSetHandler[Option[Long]] =
    query match {
      case _: VerticaNumericValuesQuery  => new LongValueResultHandler

      case verticaTable: VerticaDatetimeValuesQuery =>
        val table = verticaTable.table
        val parititions = table.columns
        val numberOfColumns = parititions.length
        val format = parititions.map(_.columnFormat).mkString(":")
        new ListStringResultHandler(numberOfColumns, format)

      case _: PrestoNumericValuesQuery    => new LongValueResultHandler

      case prestoTable: PrestoDatetimeValuesQuery   =>
        val table = prestoTable.table
        val parititions = table.columns
        val numberOfColumns = parititions.length
        val format = parititions.map(_.columnFormat).mkString(":")
        new ListStringResultHandler(numberOfColumns, format)
  }

}