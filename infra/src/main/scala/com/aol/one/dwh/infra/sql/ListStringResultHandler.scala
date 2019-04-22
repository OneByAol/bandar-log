package com.aol.one.dwh.infra.sql

import java.sql.ResultSet

import com.aol.one.dwh.infra.parser.StringToTimestampParser
import com.aol.one.dwh.infra.util.LogTrait
import org.apache.commons.dbutils.ResultSetHandler

class ListStringResultHandler(numberOfPartitions: Int, format: String) extends ResultSetHandler[Option[Long]] with LogTrait {

  override def handle(resultSet: ResultSet): Option[Long] = {

   val result =  Iterator
    .continually(resultSet.next)
    .takeWhile(identity)
    .map { _ => getColumnValues(numberOfPartitions, resultSet) }.toList

    val maxValue = parseValuesToTimestamp(result, format)

    maxValue
  }

  private def getColumnValues(numberOfPartitions: Int, resultSet: ResultSet): String = {
    (1 to numberOfPartitions)
      .map( index => resultSet.getString(index))
      .toList
      .mkString(":")
  }

  private def parseValuesToTimestamp(values: List[String], format: String): Option[Long] = {
    values
      .map(value => StringToTimestampParser.parse(value, format))
      .max
  }

}