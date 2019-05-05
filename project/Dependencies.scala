/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import sbt._

object V {
  val presto              = "0.181"
  val consulClient        = "1.2.1"
  val scalaCache          = "0.9.4"
  val scopt               = "3.7.1"
  val hikariPool          = "3.3.1"
  val scalaArm            = "2.0"
  val slf4j               = "1.7.16"
  val scalatest           = "3.0.7"
  val mockito             = "1.10.19"
  val awsGlue             = "1.11.388"
  val kafka               = "2.2.0"
  val pdMetrics           = "2.1.0"
}

object Dependencies {
  val typesafeConfig        = "com.typesafe"             %  "config"                       % "1.3.4"
  val presto                = "com.facebook.presto"      %  "presto-jdbc"                  % V.presto
  val consulClient          = "com.ecwid.consul"         %  "consul-api"                   % V.consulClient
  val hikariPool            = "com.zaxxer"               %  "HikariCP"                     % V.hikariPool
  val slf4j                 = "org.slf4j"                %  "slf4j-api"                    % V.slf4j
  val log4j                 = "org.slf4j"                %  "slf4j-log4j12"                % V.slf4j
  val scalaCache            = "com.github.cb372"         %% "scalacache-guava"             % V.scalaCache
  val scopt                 = "com.github.scopt"         %% "scopt"                        % V.scopt
  val scalaArm              = "com.jsuereth"             %% "scala-arm"                    % V.scalaArm
  val awsGlue               = "com.amazonaws"            %  "aws-java-sdk-glue"            % V.awsGlue
  val kafka4scala           = "org.apache.kafka"         %% "kafka"                        % V.kafka
  val kafkaClients          = "org.apache.kafka"         %  "kafka-clients"                % V.kafka
  val metricsApi            = "com.pagerduty"            %% "metrics-api"                  % V.pdMetrics
  val ddMetrics             = "com.pagerduty"            %% "metrics-dogstatsd"            % V.pdMetrics
  val parserCombinators     = "org.scala-lang.modules"   %% "scala-parser-combinators"     % "1.1.2"
  val commonsLang           = "org.apache.commons"       %  "commons-lang3"                % "3.9"
  val dbUtils               = "commons-dbutils"          %  "commons-dbutils"              % "1.5"
  
  val scalaTest             = "org.scalatest"            %% "scalatest"                    % V.scalatest     % Test
  val mockito               = "org.mockito"              %  "mockito-core"                 % V.mockito       % Test
}
