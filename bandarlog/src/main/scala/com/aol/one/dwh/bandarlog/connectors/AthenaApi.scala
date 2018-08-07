package com.aol.one.dwh.bandarlog.connectors

import java.sql._

import com.aol.one.dwh.infra.util.LogTrait
import com.simba.athena.amazonaws.services.glue.AWSGlueClient
import com.simba.athena.amazonaws.services.glue.model.{GetPartitionsRequest, Partition}

import scala.collection.JavaConversions._

/**
  * Athena Connector
  *
  * Provides access to the metadata table that represents your data, including its schema
  */
class AthenaApi extends LogTrait {

  private val DB_DRIVER: String = "com.simba.athena.jdbc.Driver"
  private val DB_CONNECTION: String = "jdbc:awsathena://AwsRegion=eu-central-1;" +
    "UID=xxx;" +
    "PWD=xxx;" +
    "S3OutputLocation=s3://xxx;"
  Class.forName(DB_DRIVER)
  val connection: Connection = DriverManager.getConnection(DB_CONNECTION)
  val stmt: Statement = connection.createStatement()

  val db_name = "Athena"

  private def getPartitions(table_name: String): List[Partition] = {
    val glue = AWSGlueClient.builder().withRegion("eu-central-1").build()
    val request = new GetPartitionsRequest
    request.setDatabaseName(db_name)
    request.setTableName(table_name)
    glue.getPartitions(request).getPartitions.toList
  }

  def getMaxBatchIdPartition(table_name: String): Long = {
    val partitions = getPartitions(table_name).sortWith(_.getValues.get(0) > _.getValues.get(0))
    val maxBatchId = partitions.head.getValues().get(0).toLong
    logger.info(s"Max batch_id for table $table_name is $maxBatchId")
    maxBatchId
  }
}
