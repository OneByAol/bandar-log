/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import sbt.Keys._
import sbt._

object ResolversPlugin extends AutoPlugin {

  private val typeSafeResolver   = "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"
  private val sprayResolver      = "spray repo"                   at "http://repo.spray.io"
  private val pdResolver         = "bintray-pagerduty-oss-maven"  at "https://dl.bintray.com/pagerduty/oss-maven"

  private val bintraySbtResolver = Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/sbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

  override lazy val projectSettings = Seq(
    resolvers ++= Seq(
      DefaultMavenRepository,
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      typeSafeResolver,
      sprayResolver,
      pdResolver,
      bintraySbtResolver
    )
  )

}
