package com.aol.one.dwh.infra.sql.query

import com.aol.one.dwh.infra.config.{DatetimeColumn, NumericColumn}
import com.aol.one.dwh.infra.sql._
import com.aol.one.dwh.infra.sql.pool.SqlSource._

object QueryMaker {

  implicit object NumericQueryMaker extends GenericQuery[NumericColumn] {
    override def getQuery(source: String, table: NumericColumn): Query = source match {
      case PRESTO  => PrestoNumericValuesQuery(table)
      case VERTICA => VerticaNumericValuesQuery(table)
      case s => throw new IllegalArgumentException(s"Can't get query for source:[$s]")
    }
  }

  implicit object DatetimeQueryMaker extends GenericQuery[DatetimeColumn] {

    override def getQuery(source: String, table: DatetimeColumn): Query = source match {
      case PRESTO  => PrestoDatetimeValuesQuery(table)
      case VERTICA => VerticaDatetimeValuesQuery(table)
      case s => throw new IllegalArgumentException(s"Can't get query for source:[$s]")
    }
  }
}
