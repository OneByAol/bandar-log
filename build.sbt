import Dependencies._
import sbt.Keys._

lazy val buildNumber = sys.env.getOrElse("TRAVIS_BUILD_NUMBER", "SNAPSHOT")

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / fork := true
ThisBuild / crossPaths := false
ThisBuild / updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true)

ThisBuild / organization := "com.aol.one.dwh"
ThisBuild / version := s"2.0.$buildNumber"

// Projects
val `bandar-log` = project.in(file("."))
  .enablePlugins(
    UniversalDeployPlugin
    && CodeStylePlugin
  )
  .settings(
    topLevelDirectory := None,
    publish / skip := false
  )
  .aggregate(
    `infra`,
    `bandarlog`
  )

lazy val `infra` = project
  .enablePlugins(
    CodeStylePlugin
    && ResolversPlugin
  )
  .settings(
    autoScalaLibrary := true,
    exportJars := true,
    libraryDependencies ++=
      Seq(
        slf4j,
        log4j,
        scalaTest,
        mockito,
        typesafeConfig,
        consulClient,
        presto,
        dbUtils,
        scopt,
        hikariPool,
        scalaArm,
        scalaz,
        awsGlue,
        scalaCache)
      ++ Seq(
        kafka4scala,
        kafkaClients)
      ++ Seq(
        metricsApi,
        ddMetrics)
  )

lazy val `bandarlog` = project
  .enablePlugins(
    CodeStylePlugin
    && DockerSupportPlugin
    && ResolversPlugin
  )
  .dependsOn(`infra`)
  .settings(
    mainClass in Compile := Some("com.aol.one.dwh.bandarlog.EntryPoint"),
    exportJars := true,
    libraryDependencies ++=
      Seq(
        scalaTest,
        mockito,
        scalaCache
      ),

    dockerBaseImage := "openjdk:8-jre-slim",
    dockerEntrypoint := Seq("bin/start.sh")
  )
