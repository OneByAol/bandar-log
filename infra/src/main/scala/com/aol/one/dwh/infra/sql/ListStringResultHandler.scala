package com.aol.one.dwh.infra.sql

import java.sql.ResultSet

import org.apache.commons.dbutils.ResultSetHandler

class ListStringResultHandler(numberOfPartitions: Int) extends ResultSetHandler[Option[List[String]]] {

  override def handle(resultSet: ResultSet): Option[List[String]] = {

   val result =  Iterator
    .continually(resultSet.next)
    .takeWhile(identity)
    .map { _ => getColumnValues(numberOfPartitions, resultSet) }.toList
    Option(result)
  }

  private def getColumnValues(numberOfPartitions: Int, resultSet: ResultSet): String = {
    val values = for {
      index <- 1 to numberOfPartitions
    } yield {
      resultSet.getString(index)
    }
    values.toList.mkString(":")
  }

}

