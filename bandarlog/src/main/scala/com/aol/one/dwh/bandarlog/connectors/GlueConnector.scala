/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import java.util.concurrent.Executors

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.model.{GetPartitionsRequest, GetTableRequest, Partition, Segment}
import com.aol.one.dwh.infra.config._
import com.aol.one.dwh.infra.parser.StringToTimestampParser
import com.aol.one.dwh.infra.util.LogTrait

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

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
  private val fetchSize = config.maxFetchSize

  /**
    * Calculates value in partition column (max batchId), allowing multiple requests to segments to be executed in parallel
    *
    * @param tableName  - table name
    * @param columnName - column name
    * @return           - max value in partition column (max batchId)
    */
  def getMaxPartitionValue(table: TableColumn): Long = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(threadPool)

    val futures = (0 until segmentTotalNumber) map { number =>
      Future {
        getMaxValuePerSegment(
          table,
          number)
      }
    }
    val maxValue: Long = Await.result(Future.sequence(futures), config.maxWaitTimeout).max
    logger.info(s"Max value in table ${table.table} is: $maxValue")
    maxValue
  }

  private def createPartitionsRequest(config: GlueConfig, tableName: String): GetPartitionsRequest = {
    val request = new GetPartitionsRequest
    request.setDatabaseName(config.dbname)
    request.setTableName(tableName)
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
    * Calculates max value in numeric Partition list returned from a single request
    *
    * @param tableName   - table name
    * @param tableColumn - column name
    * @param partitions  - list of Partitions
    * @return            - max value in Partition list
    */
  private def partialNumericMax(tableName: String, tableColumn: String, partitions: List[Partition]): Long = {
    val columns = getPartitionColumns(tableName)
    val values = partitions.map(_.getValues)

    val columnsData = values
      .flatMap(value => value.zip(columns))
      .filter { case (value, columnName) => columnName == tableColumn }

    columnsData match {
      case Nil => throw new IllegalArgumentException(s"Column $tableColumn not found in table $tableName.")
      case nonEmptyList: List[(String, String)] =>
        nonEmptyList.map { case (value, columnName) => value.toLong }.max
    }
  }

  /**
    * Calculates max value in nonnumeric Partition list returned from a single request
    * @param tableName    - table name
    * @param columnNames  - column names
    * @param formats      - format for parsing date values in partitions
    * @param partitions   - list of Partitions
    * @return             - max value in Partition list
    */
  private def partialDatetimeMax(tableName: String, columnNames: List[String], formats: List[String], partitions: List[Partition]): Long = {
    val allPartitions = getPartitionColumns(tableName)
    val values = partitions.map(_.getValues)
    val format = formats.mkString(":")

    val partitionsData = values
      .map(value => allPartitions.zip(value).toMap)

    val filteredPartitionsData = partitionsData
      .map(data => filterPartitions(columnNames, data, tableName))

    val jointPartitionsValues = filteredPartitionsData.map(_.values.mkString(":"))
    val formattedValues = for {
      value <- jointPartitionsValues
      longValue = StringToTimestampParser.parse(value, format)
    } yield {
      longValue
    }

    formattedValues.map(_.getOrElse(0L)).max

  }

  private def filterPartitions(columnNames: List[String], data: Map[String, String], tableName: String): Map[String, String] = {
    columnNames.map { columnName =>
      columnName -> data.getOrElse(columnName, throw new IllegalArgumentException(s"Column $columnName not found in table $tableName.")) }.toMap
  }

  /**
    * Calculates max value in Partition list in one segment of table
    *
    * @param tableName     - table name
    * @param columnName    - column name
    * @param segmentNumber - index number of the segment
    * @return              - max value in Partition list in one segment of table
    */
  private def getMaxValuePerSegment(table: TableColumn, segmentNumber: Int): Long = {

    val request = createPartitionsRequest(config, table.table)
    val segment = createSegment(segmentTotalNumber).withSegmentNumber(segmentNumber)
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
        val maxValue = table.formats match {
          case None => partialNumericMax(table.table, table.columns.head, partitions)
          case Some(formats) =>
            partialDatetimeMax(
              table.table,
              table.columns,
              table.formats.get,
              partitions)
        }
        val result = previousMax.max(maxValue)
        maxBatchIdPerRequest(token, result, request, segment)
      } else {
        (token, previousMax)
      }
    }

    if (firstFetch.nonEmpty) {
      val firstMax = table.formats match {
        case None => partialNumericMax(table.table, table.columns.head, firstFetch)
        case Some(formats) =>
          partialDatetimeMax(
            table.table,
            table.columns,
            table.formats.get,
            firstFetch)
      }
      val (_, max) = maxBatchIdPerRequest("", firstMax, request, segment)
      max
    } else {
      0
    }
  }
}
