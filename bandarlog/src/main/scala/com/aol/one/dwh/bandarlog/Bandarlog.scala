/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.bandarlog.metrics.MetricProvider
import com.aol.one.dwh.bandarlog.scheduler.Scheduler
import com.aol.one.dwh.infra.util.{ExceptionPrinter, LogTrait}
import com.pagerduty.metrics.Metrics
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Bandarlog
  *
  * The main component which is responsible for the managing data providers and reporters
  * [data source] --> [connector] --> [provider] --> [BANDARLOG] --> [reporter] --> [monitor]
  */
class Bandarlog[V](
    providers: Seq[MetricProvider[V]],
    reporters: Seq[Metrics],
    scheduler: Scheduler
  ) extends LogTrait with ExceptionPrinter {

  def execute(): Unit = {
    scheduler.schedule(() =>
      providers.foreach { metricProvider =>
        val metric = metricProvider.metric
        val provider = metricProvider.provider

        Try {
          val value = provider.provide()
          metric.value.setValue(value.getValue)
        }.recover {
          case NonFatal(e) =>
            logger.error("Catching exception {}", ExceptionUtils.getStackTrace(e))
            metric.value.setValue(None)
        }

        logger.info(s"Metric:[${metric.prefix}.${metric.name}] Tags:[${metric.tags.mkString(",")}] Value:[${metric.value.getValue}]")

        reporters.foreach { reporter =>
          val gaugeName = s"${metric.prefix}.${metric.name}"
          val gaugeTags = metric.tags.map { tag => tag.key -> tag.value}

          metric.value.getValue.foreach {
            case value: Long =>
              reporter.gauge(gaugeName, value, gaugeTags: _*)
              logger.trace(s"Metric:[${metric.prefix}.${metric.name}] reported.")
            case _ =>
              logger.warn(s"Metric:[${metric.prefix}.${metric.name}] is of type ${metric.value.getValue.get.getClass} which could not be reported.")
          }
        }
      })
  }

  def shutdown(): Unit = {
    reporters.foreach(_.stop())
    scheduler.shutdown()
  }
}
