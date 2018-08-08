package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.aws.BandarlogAWSCredentialsProvider
import com.aol.one.dwh.infra.config.GlueConfig
import com.aol.one.dwh.infra.util.LogTrait
import com.simba.athena.amazonaws.services.glue.AWSGlueClient
import com.simba.athena.amazonaws.services.glue.model.{GetPartitionsRequest, GetTableRequest, Partition}

import scala.collection.JavaConversions._

/**
  * Glue Connector
  *
  * Provides access to the metadata table
  */
class GlueConnector(config: GlueConfig) extends LogTrait {

  private val credentialProvider = new BandarlogAWSCredentialsProvider(config: GlueConfig)
  private val glueClient = AWSGlueClient.builder()
    .withRegion(config.region)
    .withCredentials(credentialProvider)
    .build()

  /**
    * @param tableName - name of table
    * @return - list of partitions' column names from glue metadata table
    */
  private def getPartitionColumns(tableName: String): List[String] = {
    val table = new GetTableRequest
    table.setDatabaseName(config.database)
    table.setName(tableName)
    val columns = glueClient.getTable(table).getTable.getPartitionKeys.map(f => f.getName).toList
    columns
  }

  /**
    * @param tableName - name of table
    * @return - list of Partiton objects which contain metadata
    */
  private def getPartitions(tableName: String): List[Partition] = {
    val request = new GetPartitionsRequest
    request.setDatabaseName(config.database)
    request.setTableName(tableName)
    glueClient.getPartitions(request).getPartitions.toList
  }

  /**
    * @param tableName   - name of table
    * @param tableColumn - name of partition column
    * @return - max value in partition column
    */
  def getMaxBatchId(tableName: String, tableColumn: String): Long = {

    val columns = getPartitionColumns(tableName)
    val values = getPartitions(tableName).map(p => p.getValues)
    val maxBatchId = values.flatMap(elem => elem.zip(columns))
      .filter(tuple => tuple._2 == tableColumn)
      .map(x => x._1.toLong).max
    maxBatchId
  }
}
