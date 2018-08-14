package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.{GlueConnector, JdbcConnector}
import com.aol.one.dwh.bandarlog.providers.SqlProvider.TimestampProvider
import com.aol.one.dwh.infra.config.{ConnectorConfig, TableColumn}
import com.aol.one.dwh.infra.sql.MaxValuesQuery
import com.aol.one.dwh.infra.sql.pool.SqlSource.{GLUE, PRESTO, VERTICA}
import com.typesafe.config.Config
import com.aol.one.dwh.infra.config.RichConfig._
import com.aol.one.dwh.infra.sql.pool.ConnectionPoolHolder

class ProviderFactory(mainConfig: Config, connectionPoolHolder: ConnectionPoolHolder) {

  def getProvider(connector: ConnectorConfig, table: TableColumn): TimestampProvider = {
    connector.connectorType match {

      case VERTICA | PRESTO => {
        val query = MaxValuesQuery.get(connector.connectorType)(table)
        val connectionPool = connectionPoolHolder.get(connector)
        val provider = new SqlTimestampProvider(JdbcConnector(connector.connectorType, connectionPool), query)
        provider
      }

      case GLUE =>
        val glueConfig = mainConfig.getGlueConfig(connector.configId)
        val glueConnector = new GlueConnector(glueConfig)
        val provider = new GlueTimestampProvider(glueConnector, table)
        provider
    }
  }
}
