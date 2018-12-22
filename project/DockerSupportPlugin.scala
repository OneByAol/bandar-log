/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import com.typesafe.sbt.GitVersioning
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport.Universal
import sbt.Keys._
import sbt._
import com.typesafe.sbt.packager.Keys.packageName
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.universal.UniversalDeployPlugin
import Path.rebase

object DockerSupportPlugin extends AutoPlugin {

  override def requires: sbt.Plugins = JavaAppPackaging && GitVersioning && UniversalDeployPlugin

  val organizationName = "onebyaol"

  override lazy val projectSettings = Seq(
    version in Docker := s"${version.value}",
    dockerUpdateLatest := true,
    packageName := s"$organizationName/${packageName.value}",
    mappings in Universal ++= {
    val dir = baseDirectory.value / "scripts"
    (( dir * "*") pair rebase(dir, "bin/"))
  })

}
