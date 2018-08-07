package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.aws.BandarlogAWSCredentialsProvider
import com.aol.one.dwh.infra.config.GlueConfig
import com.aol.one.dwh.infra.util.LogTrait
import com.simba.athena.amazonaws.services.glue.AWSGlueClient
import com.simba.athena.amazonaws.services.glue.model.{GetPartitionsRequest, Partition}

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

  private def getPartitions(table_name: String): List[Partition] = {

    val request = new GetPartitionsRequest
    request.setDatabaseName(config.database)
    request.setTableName(table_name)
    glueClient.getPartitions(request).getPartitions.toList
  }

  def getMaxBatchId(table_name: String): Long = {
    val sortByValue = (a: Partition, b: Partition) => a.getValues.get(0) > b.getValues.get(0)
    val partitions = getPartitions(table_name).sortWith(sortByValue)
    val maxBatchId = partitions.head.getValues.get(0).toLong
    logger.info(s"Max batch_id for table $table_name is $maxBatchId")
    maxBatchId
  }
}
