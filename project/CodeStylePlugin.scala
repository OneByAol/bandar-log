/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import org.scalastyle.sbt.ScalastylePlugin._
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import sbt.Keys._
import sbt._

object CodeStylePlugin extends AutoPlugin {
  private lazy val configFile = {
    file("project") / "scalastyle-config.xml"
  }

  override lazy val projectSettings = Seq(

    // disabled java docs, we don't need check doc syntax and store java doc file
    sources in doc in Compile := List(),

    // by default set build as failed if scala check style errors found
    Global / excludeLintKeys += scalastyleFailOnError,
    ThisBuild / scalastyleFailOnError := true,

    Compile / scalastyleConfig := configFile
  )
}
