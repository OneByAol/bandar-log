package com.aol.one.dwh.bandarlog.connectors

import java.util.concurrent.Executors

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.aol.one.dwh.infra.config.GlueConfig
import com.aol.one.dwh.infra.util.LogTrait
import com.simba.athena.amazonaws.auth.BasicAWSCredentials
import com.simba.athena.amazonaws.services.glue.AWSGlueClient
import com.simba.athena.amazonaws.services.glue.model.{GetPartitionsRequest, GetTableRequest, Partition, Segment}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Glue Connector
  *
  * Provides access to the metadata table
  */
class GlueConnector(config: GlueConfig) extends LogTrait {

  private val credentialsProvider = new AWSCredentialsProvider {
    override def refresh(): Unit = {}

    override def getCredentials: AWSCredentials = new BasicAWSCredentials(config.accessKey, config.secretKey)
  }
  private val glueClient = AWSGlueClient.builder()
    .withRegion(config.region)
    .withCredentials(credentialsProvider)
    .build()
  private val segmentTotalNumber = config.segmentTotalNumber
  private val threadPool = Executors.newCachedThreadPool()

  private def createPartitionsRequest(config: GlueConfig): GetPartitionsRequest = {
    val request = new GetPartitionsRequest
    request.setDatabaseName(config.dbname)
    request
  }

  private def createSegment(number: Int): Segment = {
    val segment = new Segment()
    segment.setTotalSegments(number)
    segment
  }

  /**
    * Fetches list of names of partition columns in table
    *
    * @param tableName - table name
    * @return          - list of names of partition columns
    */
  private def getPartitionColumns(tableName: String): List[String] = {
    val tableRequest = new GetTableRequest
    tableRequest.setDatabaseName(config.dbname)
    tableRequest.setName(tableName)
    glueClient.getTable(tableRequest).getTable.getPartitionKeys.map(_.getName).toList
  }

  /**
    * Calculates max value in Partition list
    *
    * @param tableName   - table name
    * @param tableColumn - column name
    * @param list        - list of Partitions
    * @return            - max value in Partition list
    */
  private def temporaryMax(tableName: String, tableColumn: String, list: List[Partition]): Long = {
    val columnNames = getPartitionColumns(tableName)
    val batchIdValues = list.map(_.getValues)
    batchIdValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == tableColumn }
      .map { case (value, columnName) => value.toLong }
      .max
  }

  /**
    * Calculates max value in Partition list in one segment of table
    *
    * @param tableName  - table name
    * @param columnName - column name
    * @param request    - request for getting partitions
    * @param segment    - segment - a non-overlapping region of a table's partitions
    * @return           - max value in Partition list in one segment of table
    */
  private def getMaxBatchIdPerSegment(tableName: String, columnName: String, request: GetPartitionsRequest, segment: Segment): Long = {
    request.setTableName(tableName)
    val fetchSize = config.maxFetchSize
    val firstFetch = glueClient.getPartitions(request.withSegment(segment).withMaxResults(fetchSize)).getPartitions.toList

    @tailrec
    def maxBatchIdPerRequest(token: String, previousMax: Long, request: GetPartitionsRequest, segment: Segment): (String, Long) = {
      val token = glueClient.getPartitions(request.withSegment(segment).withMaxResults(fetchSize)).getNextToken
      val partitions = glueClient.getPartitions(
        request
          .withSegment(segment)
          .withNextToken(token)
          .withMaxResults(fetchSize))
        .getPartitions.toList

      if (partitions.nonEmpty) {
        val maxValue = temporaryMax(tableName, columnName, partitions)
        val result = previousMax.max(maxValue)
        maxBatchIdPerRequest(token, result, request, segment)
      } else {
        (token, previousMax)
      }
    }

    if (firstFetch.nonEmpty) {
      val firstMax = temporaryMax(tableName, columnName, firstFetch)
      val (_, max) = maxBatchIdPerRequest("", firstMax, request, segment)
      max
    } else {
      0
    }
  }

  /**
    * Calculates value in partition column (max batchId), allowing multiple requests to segments to be executed in parallel
    *
    * @param tableName  - table name
    * @param columnName - column name
    * @return           - max value in partition column (max batchId)
    */
  def getMaxBatchId(tableName: String, columnName: String): Long = {

    val request = new ThreadLocal[GetPartitionsRequest] {
      override def initialValue(): GetPartitionsRequest = createPartitionsRequest(config)
    }

    val segment = new ThreadLocal[Segment] {
      override def initialValue(): Segment = createSegment(segmentTotalNumber)
    }

    implicit val ec = ExecutionContext.fromExecutor(threadPool)

    val futures = (0 until segmentTotalNumber) map { number =>
      Future {
        getMaxBatchIdPerSegment(
          tableName,
          columnName,
          request.get(),
          segment.get().withSegmentNumber(number))
      }
    }
    val maxBatchId: Long = Await.result(Future.sequence(futures), config.maxWaitTimeout).max
    logger.info(s"Max batchId in table $tableName is: $maxBatchId")
    maxBatchId
  }
}
