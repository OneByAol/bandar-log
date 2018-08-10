package com.aol.one.dwh.bandarlog.connectors

import com.aol.one.dwh.infra.aws.BandarlogAWSCredentialsProvider
import com.aol.one.dwh.infra.config.GlueConfig
import com.aol.one.dwh.infra.util.LogTrait
import com.simba.athena.amazonaws.services.glue.AWSGlueClient
import com.simba.athena.amazonaws.services.glue.model.{GetPartitionsRequest, GetTableRequest, Partition, Segment}

import scala.annotation.tailrec
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
  private val fetchSize = config.fetchSize
  private val request = new GetPartitionsRequest
  private val segment = new Segment()

  segment.setTotalSegments(1)

  private def getPartitionColumns(tableName: String): List[String] = {
    val table = new GetTableRequest
    table.setDatabaseName(config.database)
    table.setName(tableName)
    val columns = glueClient.getTable(table).getTable.getPartitionKeys.map(f => f.getName).toList
    columns
  }

  private def temporaryMax(tableName: String, tableColumn: String, list: List[Partition]): Long = {
    val columns = getPartitionColumns(tableName)
    val values = list.map(p => p.getValues)
    val r = values.flatMap(x => x.zip(columns)).filter(tuple => tuple._2 == tableColumn).map(x => x._1.toLong).max
    r
  }

  def getMaxBatchId(tableName: String, columnName: String): Long = {
    @tailrec
    def getMaxInSegment(token: String, previousMax: Long): (String, Long) = {
      val token = glueClient.getPartitions(request.withMaxResults(fetchSize)).getNextToken
      val values = glueClient.getPartitions(request.withNextToken(token).withMaxResults(fetchSize)).getPartitions.toList
      if (values.nonEmpty) {
        val maxValue = temporaryMax(tableName, columnName, values)
        val res = previousMax.max(maxValue)
        getMaxInSegment(token, res)
      } else {
        (token, previousMax)
      }
    }

    val (_, max) = getMaxInSegment("", 0)
    max
  }

}
