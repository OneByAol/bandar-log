/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import com.aol.one.dwh.infra.config.Tag
import com.aol.one.dwh.infra.util.EnvUtil
import com.typesafe.config.Config
import com.aol.one.dwh.infra.config.RichConfig._

/**
  * Custom tags builder
  *
  * Provides list of custom tags
  */
object CustomTags {

  def apply(bandarlogConf: Config): List[Tag] = {
    EnvUtil.getEnvironment.map(env => Tag("env", env)).toList ++ bandarlogConf.getCustomTags
  }
}

/**
  * Transform tags according to format function
  */
object TagsFormatter {
}
