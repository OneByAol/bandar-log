/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import java.net.InetAddress
import java.util.concurrent.{TimeUnit, _}

import com.aol.one.dwh.infra.config.{PushGatewayConfig, ReportConfig}
import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.PushGateway

import scala.util.control.NonFatal

/**
  * PushGateway Metric Reporter
  */
class PushGatewayMetricReporter(config: PushGatewayConfig, tags: List[String], metricRegistry: MetricRegistry, reportConf: ReportConfig)
  extends MetricReporter with LazyLogging {

  private lazy val executor = new ScheduledThreadPoolExecutor(1)

  val host: String = config.host.getOrElse(InetAddress.getLocalHost.getHostName)
  val pushJob: String = config.pushJob.getOrElse(s"bandarlog-${reportConf.prefix}-push-job")
  val port: Int = config.port.getOrElse(9091)

  private lazy val pushGateway = {
    new PushGateway(s"$host:$port")
  }

  private lazy val collectorRegistry = {
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry))
    CollectorRegistry.defaultRegistry
  }

  override def start(): Unit = {
    val task = new Runnable {
      def run(): Unit = {
        try {
          pushGateway.pushAdd(collectorRegistry, reportConf.prefix)
        } catch {
          case NonFatal(e) => logger.error("Failed to report metrics to PushGateway, error below", e)
        }
      }
    }
    executor.scheduleAtFixedRate(task, reportConf.interval, reportConf.interval, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    executor.shutdownNow()
  }

}
