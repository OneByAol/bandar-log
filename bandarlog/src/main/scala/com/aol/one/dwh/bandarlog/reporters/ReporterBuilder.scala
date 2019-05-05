/*
  ******************************************************************************
  * Copyright 2019, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import java.net.InetAddress

import com.aol.one.dwh.infra.config.{ReportConfig, ReporterConfig, Tag}
import com.aol.one.dwh.infra.config.RichConfig._
import com.pagerduty.metrics.Metrics
import com.pagerduty.metrics.pdstats.DogstatsdMetrics
import com.typesafe.config.Config

object ReporterBuilder {

  def build(reporter: ReporterConfig, tags: List[Tag], mainConf: Config, reportConf: ReportConfig): Metrics = {
    reporter.reporterType match {
      case "datadog" =>
        val standardTags = tags.map(tag => tag.key -> tag.value)
        val datadogConfig = mainConf.getDatadogConfig(reporter.configId)
        val hostname = datadogConfig.host.getOrElse(InetAddress.getLocalHost.getHostName)

        new DogstatsdMetrics(reportConf.prefix, hostname, standardTags :_*)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported reporter:[$reporter]")
    }
  }
}
