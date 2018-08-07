/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import sbt._

object V {
  val spark               = "1.5.2"
  val config              = "1.3.0"
  val datadogMetrics      = "1.1.2"
  val dbUtils             = "1.5"
  val presto              = "0.181"
  val consulClient        = "1.2.1"
  val scalaCache          = "0.9.4"
  val scopt               = "3.3.0"
  val hikariPool          = "2.7.8"
  val scalaArm            = "2.0"
  val scalaz              = "7.2.9"
  val slf4j               = "1.7.16"
  val scalatest           = "2.2.6"
  val mockito             = "1.10.19"
  val athenaDriver        = "2.0.2"
}

object Dependencies {
  val typesafeConfig        = "com.typesafe"          % "config"                       % V.config
  val datadogMetrics        = "org.coursera"          % "metrics-datadog"              % V.datadogMetrics
  val dbUtils               = "commons-dbutils"       % "commons-dbutils"              % V.dbUtils
  val presto                = "com.facebook.presto"   % "presto-jdbc"                  % V.presto
  val consulClient          = "com.ecwid.consul"      % "consul-api"                   % V.consulClient
  val hikariPool            = "com.zaxxer"            % "HikariCP"                     % V.hikariPool
  val slf4j                 = "org.slf4j"             % "slf4j-api"                    % V.slf4j
  val log4j                 = "org.slf4j"             % "slf4j-log4j12"                % V.slf4j
  val scalaCache            = "com.github.cb372"      %% "scalacache-guava"            % V.scalaCache
  val scopt                 = "com.github.scopt"      %% "scopt"                       % V.scopt
  val sparkStreaming        = "org.apache.spark"      %% "spark-streaming"             % V.spark
  val sparkStreamingKafka   = "org.apache.spark"      %% "spark-streaming-kafka"       % V.spark
  val scalaArm              = "com.jsuereth"          %% "scala-arm"                   % V.scalaArm
  val scalaz                = "org.scalaz"            %% "scalaz-core"                 % V.scalaz
  val scalaTest             = "org.scalatest"         %% "scalatest"                   % V.scalatest     % Test
  val mockito               = "org.mockito"           % "mockito-core"                 % V.mockito       % Test
  val athenaDriver          = "com.syncron.amazonaws" % "simba-athena-jdbc-driver"     % V.athenaDriver
}
