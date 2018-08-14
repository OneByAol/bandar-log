package com.aol.one.dwh.bandarlog.connectors

import java.util.concurrent.{Callable, ExecutorService, Executors}

import com.aol.one.dwh.infra.aws.BandarlogAWSCredentialsProvider
import com.aol.one.dwh.infra.config.{GlueConfig, SchedulerConfig}
import com.aol.one.dwh.infra.util.LogTrait
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
class GlueConnector(config: GlueConfig, configScheduler: SchedulerConfig) extends LogTrait {

  private val credentialProvider = new BandarlogAWSCredentialsProvider(config: GlueConfig)

  private val glueClient = AWSGlueClient.builder()
    .withRegion(config.region)
    .withCredentials(credentialProvider)
    .build()

  private val fetchSize = config.fetchSize

  private val request = new GetPartitionsRequest
  request.setDatabaseName(config.database)

  val segmentTotalNumber = 10
  private val segment = new Segment()
  segment.setTotalSegments(segmentTotalNumber)

  private val pool: ExecutorService = Executors.newFixedThreadPool(segmentTotalNumber)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  /**
    * Fetches list of names of partition columns in table
    *
    * @param tableName - table name
    * @return - list of names of partition columns
    */

  private def getPartitionColumns(tableName: String): List[String] = {
    val table = new GetTableRequest
    table.setDatabaseName(config.database)
    table.setName(tableName)
    val columns = glueClient.getTable(table).getTable.getPartitionKeys.map(f => f.getName).toList
    columns
  }

  /**
    * Calculates max value in Partition list
    *
    * @param tableName   - table name
    * @param tableColumn - column name
    * @param list        - list of Partitions
    * @return - max value in Partition list
    */

  private def temporaryMax(tableName: String, tableColumn: String, list: List[Partition]): Long = {
    val columns = getPartitionColumns(tableName)
    val values = list.map(p => p.getValues)
    val r = values.flatMap(x => x.zip(columns)).filter(tuple => tuple._2 == tableColumn).map(x => x._1.toLong).max
    r
  }

  /**
    * Calculates max value in Partition list in one segment of table
    *
    * @param tableName     - table name
    * @param columnName    - column name
    * @param segmentNumber - number of segment
    * @return - max value in Partition list in one segment of table
    */
  private def getMaxBatchIdPerSegment(tableName: String, columnName: String, segmentNumber: Int): Long = {
    request.setTableName(tableName)
    segment.withSegmentNumber(segmentNumber)
    val fistFetch = glueClient.getPartitions(request.withSegment(segment).withMaxResults(fetchSize)).getPartitions.toList

    @tailrec
    def maxBatchIdPerRequest(token: String, previousMax: Long): (String, Long) = {
      val token = glueClient.getPartitions(request.withSegment(segment).withMaxResults(fetchSize)).getNextToken
      val values = glueClient.getPartitions(request.withSegment(segment).withNextToken(token).withMaxResults(fetchSize)).getPartitions.toList
      // println(values.map(x => x.getValues))
      if (values.nonEmpty) {
        val maxValue = temporaryMax(tableName, columnName, values)
        val res = previousMax.max(maxValue)
        // println(res)
        maxBatchIdPerRequest(token, res)
      } else {
        (token, previousMax)
      }
    }

    if (fistFetch.nonEmpty) {
      val firstMax = temporaryMax(tableName, columnName, fistFetch)
      val (_, max) = maxBatchIdPerRequest("", firstMax)
      max
    } else {
      0
    }
  }

  /**
    * Calculates value in partition column (max batchId)
    *
    * @param tableName  - table name
    * @param columnName - column name
    * @return - max value in partition column (max batchId)
    */

  def getMaxBatchId(tableName: String, columnName: String): Long = {
    val futures: IndexedSeq[Future[Long]] = (0 until segmentTotalNumber) map { number =>
      Future {
        pool.submit(
          new Callable[Long] {
            override def call(): Long = {
              getMaxBatchIdPerSegment(tableName, columnName, number)
            }
          }
        ).get()
      }
    }
    val results = Future.sequence(futures)
    Await.result(results, configScheduler.schedulingPeriod).max
  }

}
