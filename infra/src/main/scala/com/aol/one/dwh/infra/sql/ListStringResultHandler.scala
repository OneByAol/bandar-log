package com.aol.one.dwh.infra.sql

import java.sql.ResultSet

import com.aol.one.dwh.infra.util.LogTrait
import org.apache.commons.dbutils.ResultSetHandler

class ListStringResultHandler(numberOfPartitions: Int) extends ResultSetHandler[Option[List[String]]] with LogTrait {

  override def handle(resultSet: ResultSet): Option[List[String]] = {

   val result =  Iterator
    .continually(resultSet.next)
    .takeWhile(identity)
    .map { _ => getColumnValues(numberOfPartitions, resultSet) }.toList

    Option(result)
  }

  private def getColumnValues(numberOfPartitions: Int, resultSet: ResultSet): String = {
    (1 to numberOfPartitions).map( index => resultSet.getString(index)).toList.mkString(":")
  }

}

