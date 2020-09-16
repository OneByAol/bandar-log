/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class BandarlogsFactoryTest extends FunSuite with MockitoSugar {

  private def createConfig(enabled: Boolean, bandarlogType: String): Config = {
    ConfigFactory.parseString(
      s"""
        |datadog-config {
        |  host = null
        |}
        |
        |test-config {
        |  host = ""
        |  port = 8889
        |  username = ""
        |  password = ""
        |  schema = ""
        |  dbname = ""
        |}
        |
        |bandarlogs {
        |  sql-bandarlogs {
        |
        |    enabled = $enabled
        |
        |    bandarlog-type = $bandarlogType
        |
        |    metrics = ["IN", "OUT", "LAG"]
        |
        |    reporters = [
        |      {
        |        type = "datadog"
        |        config-id = "datadog-config"
        |      }
        |    ]
        |
        |    report {
        |      prefix = "test_prefix"
        |      interval.sec = 180
        |    }
        |
        |    in-connector {
        |      type = "presto"
        |      config-id = "test-config"
        |      tag = "test-config"
        |    }
        |
        |    out-connectors = [
        |      {
        |        type = "presto"
        |        config-id = "test-config"
        |        tag = "test-config"
        |      }
        |    ]
        |
        |    scheduler {
        |      delay.seconds = 0
        |      scheduling.seconds = 60
        |    }
        |
        |    tables = [
        |      {
        |        in-table = "in_table_1:column"
        |        out-table = "out_table_1:column"
        |        tag = "custom_table_tag"
        |        in-filters = [
        |          {
        |            quoted = true
        |            key = "string_column"
        |            value = "value"
        |          },
        |          {
        |            quoted = false
        |            key = "double_column"
        |            value = 0.1
        |          }
        |        ]
        |        out-filters = [
        |          {
        |            quoted = false
        |            key = "int_column"
        |            value = 1
        |          }
        |        ]
        |      },
        |      {
        |        in-table = "in_table_2:column"
        |        out-table = "out_table_2:column"
        |      }
        |    ]
        |  }
        |}
      """.stripMargin
    ).resolve
  }

  test("filter disabled bandarlogs") {
    val config = createConfig(enabled = false, bandarlogType = "kafka")

    val resultBandarlogs = new BandarlogsFactory(config).create()

    assert(resultBandarlogs.isEmpty)
  }

  test("create bandarlogs for known bandarlog type") {
    val config = createConfig(enabled = true, bandarlogType = "sql")

    val resultBandarlogs = new BandarlogsFactory(config).create()
    assert(resultBandarlogs.size == 1)
  }

  test("skip failed bandarlogs") {
    val config = createConfig(enabled = true, bandarlogType = "unknown_type")

    val resultBandarlogs = new BandarlogsFactory(config).create()

    assert(resultBandarlogs.isEmpty)
  }
}
