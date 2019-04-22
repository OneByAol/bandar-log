package com.aol.one.dwh.infra.sql.query
import com.aol.one.dwh.infra.sql.Query

trait GenericQuery[A] {
  def getQuery(source: String, table: A): Query
}


