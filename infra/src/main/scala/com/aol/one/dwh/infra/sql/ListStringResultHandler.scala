package com.aol.one.dwh.infra.sql

import java.sql.ResultSet

import com.aol.one.dwh.infra.parser.StringToTimestampParser
import org.apache.commons.dbutils.ResultSetHandler

class ListStringResultHandler(numberOfPartitions: Int, format: String) extends ResultSetHandler[Option[Long]] {

  override def handle(resultSet: ResultSet): Option[Long] = {

   val result =  Iterator
    .continually(resultSet.next)
    .takeWhile(identity)
    .map { _ => getColumnValues(numberOfPartitions, resultSet) }.toList

    val converted = result.map(value => StringToTimestampParser.parse(value, format)).map(_.getOrElse(0L)).max

    Option(converted)
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

