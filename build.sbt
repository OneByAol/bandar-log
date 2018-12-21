import Dependencies._
import sbt.Keys._

version in ThisBuild := s"2.0.${sys.env("TRAVIS_BUILD_NUMBER")}"
scalaVersion in ThisBuild := "2.11.12"

organization in ThisBuild := "com.aol.one.dwh"
fork in ThisBuild := true
crossPaths in ThisBuild := false
updateOptions in ThisBuild := updateOptions.value.withCachedResolution(cachedResoluton = true)

// Projects
val `bandar-log` = project.in(file("."))
  .enablePlugins(UniversalDeployPlugin && CodeStylePlugin)
  .settings(
    topLevelDirectory := None,
    publish := false
  )
  .aggregate(
    `infra`,
    `bandarlog`
  )

lazy val `infra` = project.enablePlugins(CodeStylePlugin && ResolversPlugin).
  settings(
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
            sparkStreaming,
            sparkStreamingKafka,
            hikariPool,
            scalaArm,
            scalaz,
            awsGlue
        )
  )

lazy val `bandarlog` = project.enablePlugins(CodeStylePlugin && DockerSupportPlugin && ResolversPlugin).
  dependsOn(`infra`).
  settings(
      mainClass in Compile := Some("com.aol.one.dwh.bandarlog.EntryPoint"),
      exportJars := true,
      libraryDependencies ++=
        Seq(
          scalaTest,
          mockito,
          datadogMetrics,
          scalaCache
        ),

    dockerBaseImage := "java",
    dockerEntrypoint := Seq("bin/start.sh")
  )
