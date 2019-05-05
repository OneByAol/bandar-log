/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.bandarlog.connectors.KafkaConnector
import com.aol.one.dwh.bandarlog.metrics._
import com.aol.one.dwh.bandarlog.providers.ProviderFactory
import com.aol.one.dwh.bandarlog.reporters.{CustomTags, ReporterBuilder}
import com.aol.one.dwh.bandarlog.scheduler.Scheduler
import com.aol.one.dwh.infra.config.RichConfig._
import com.aol.one.dwh.infra.kafka.KafkaCluster
import com.aol.one.dwh.infra.sql.pool.ConnectionPoolHolder
import com.aol.one.dwh.infra.util.{ExceptionPrinter, LogTrait}
import com.pagerduty.metrics.Metrics
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class BandarlogsFactory(mainConfig: Config) extends LogTrait with ExceptionPrinter {

  def create(): Seq[Bandarlog[_]] = {
    val bandarlogIds = mainConfig.getObject("bandarlogs").keys.toSeq
    logger.info(s"Defined bandarlog ids:[${bandarlogIds.mkString(",")}]")

    val connectionPoolHolder = new ConnectionPoolHolder(mainConfig)

    val bandarlogs = bandarlogIds.map(id => id -> mainConfig.getConfig(s"bandarlogs.$id")).filter { case (id, bandarlogConf) =>
      val enabled = bandarlogConf.isEnabled
      logger.info(s"Bandarlog:[$id] Enabled:[$enabled]")
      enabled
    }.map { case (id, bandarlogConf) =>
      logger.info(s"Creating bandarlog:[$id]...")

      Try {
        val metricProviders = createMetricProviders(bandarlogConf, connectionPoolHolder)
        val reporters = createReporters(bandarlogConf, metricProviders.map(_.metric))
        new Bandarlog(metricProviders, reporters, new Scheduler(bandarlogConf.getSchedulerConfig))

      }.recoverWith {
        case NonFatal(e) =>
          logger.error(s"Can't create bandarlog:[$id]. Catching exception {}", e.getStringStackTrace)
          Failure(e)
      }.toOption
    }

    bandarlogs.filter(_.isDefined).map(_.get)
  }

  private def createMetricProviders(bandarlogConf: Config, connectionPoolHolder: ConnectionPoolHolder) = {
    bandarlogConf.getBandarlogType match {
      case "kafka" => kafkaMetricProviders(bandarlogConf)
      case "sql" => metricProviders(bandarlogConf, connectionPoolHolder)
      case t => throw new IllegalArgumentException(s"Unsupported bandarlog type:[$t]")
    }
  }

  private def kafkaMetricProviders(bandarlogConf: Config) = {
    val metricsPrefix = bandarlogConf.getReportConfig.prefix
    val kafkaConfig = mainConfig.getKafkaConfig(bandarlogConf.getConnector)
    val kafkaConnector = new KafkaConnector(KafkaCluster(kafkaConfig))
    val kafkaMetricFactory = new KafkaMetricFactory(kafkaConnector)

    bandarlogConf.getKafkaTopics.flatMap { topic =>
      bandarlogConf.getMetrics.map(metricId => kafkaMetricFactory.create(metricId, metricsPrefix, topic))
    }
  }

  private def metricProviders(bandarlogConf: Config, connectionPoolHolder: ConnectionPoolHolder) = {
    val metricsPrefix = bandarlogConf.getReportConfig.prefix
    val providerFactory = new ProviderFactory(mainConfig, connectionPoolHolder)
    val metricFactory = new MetricFactory(providerFactory)

    bandarlogConf.getTables.flatMap { case (inTable, outTable) =>
      bandarlogConf.getMetrics.flatMap { metricId =>
        metricFactory.create(metricId, metricsPrefix, bandarlogConf.getInConnector, bandarlogConf.getOutConnectors, inTable, outTable)
      }
    }
  }

  private def createReporters[V](bandarlogConf: Config, metrics: Seq[Metric[V]]) = {
    metrics.flatMap { metric =>
      val tags = metric.tags ++ CustomTags(bandarlogConf)

      bandarlogConf
        .getReporters
        .map { reporter => 
          ReporterBuilder
            .build(
              reporter,
              tags,
              mainConfig,
              bandarlogConf.getReportConfig)
        }
    }
  }
}
