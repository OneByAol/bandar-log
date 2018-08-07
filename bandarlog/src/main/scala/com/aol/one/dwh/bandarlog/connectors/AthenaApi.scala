package com.aol.one.dwh.bandarlog.connectors

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.aol.one.dwh.infra.util.LogTrait

class AthenaApi extends LogTrait{

  private val DB_DRIVER: String = "com.simba.athena.jdbc.Driver"
  private val DB_CONNECTION: String = "jdbc:awsathena://AwsRegion=eu-central-1;" +
    "UID=xxx;" +
    "PWD=xxx;" +
    "S3OutputLocation=s3://xxx;"
  Class.forName(DB_DRIVER)
  val connection: Connection = DriverManager.getConnection(DB_CONNECTION)
  val stmt: Statement = connection.createStatement()

  private def fetchAll(rs: ResultSet): List[String] = {
    Iterator
      .continually(rs.next)
      .takeWhile(identity)
      .map { _ => rs.getString(1) }
      .toList
  }

}
