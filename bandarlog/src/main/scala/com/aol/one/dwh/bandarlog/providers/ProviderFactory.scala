/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.{GlueConnector, JdbcConnector}
import com.aol.one.dwh.bandarlog.providers.SqlProvider.TimestampProvider
import com.aol.one.dwh.infra.config.RichConfig._
import com.aol.one.dwh.infra.config.{ConnectorConfig, Table}
import com.aol.one.dwh.infra.sql.MaxValuesQuery
import com.aol.one.dwh.infra.sql.pool.ConnectionPoolHolder
import com.aol.one.dwh.infra.sql.pool.SqlSource._
import com.typesafe.config.Config

class ProviderFactory(mainConfig: Config, connectionPoolHolder: ConnectionPoolHolder) {

  def create(connector: ConnectorConfig, table: Table): TimestampProvider = {
    connector.connectorType match {

      case VERTICA | PRESTO | MYSQL => {
        val query = MaxValuesQuery.get(connector.connectorType)(table)
        val connectionPool = connectionPoolHolder.get(connector)
        new SqlTimestampProvider(JdbcConnector(connector.connectorType, connectionPool), query)
      }

      case GLUE => {
        val glueConfig = mainConfig.getGlueConfig(connector.configId)
        val glueConnector = new GlueConnector(glueConfig)
        new GlueTimestampProvider(glueConnector, table)
      }
    }
  }
}
