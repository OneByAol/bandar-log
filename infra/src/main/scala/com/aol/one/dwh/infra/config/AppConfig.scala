/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.config

import com.aol.one.dwh.infra.consul.ConsulManager
import com.aol.one.dwh.infra.util.{EnvUtil, LogTrait}
import com.ecwid.consul.v1.ConsulClient
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import resource.managed
import java.util.regex.Pattern

import scala.io.Source

case class AppParams(
  configStrategy: String = "path",
  configPath: String = "",
  consulHost: String = "",
  consulPort: Int = 8500,
  consulConfigPath: String = ""
)

object AppConfig extends LogTrait {

  def apply(args: Array[String]): Config = resolveCkmsProperties(fetchConfig(args))

  private def fetchConfig(args: Array[String]) = readCmdParams(args) match {
    case Some(params: AppParams) =>
      params.configStrategy match {
        case "consul"     => fetchConfigFromConsul(params)
        case "path"       => fetchConfigFromPath(params)
        case "resources"  => fetchConfigFromResources
      }
    case _ => fetchConfigFromResources
  }

  private def fetchConfigFromConsul(params: AppParams): Config = {
    val consulClient = new ConsulClient(params.consulHost, params.consulPort)
    val consulManager = new ConsulManager(consulClient)
    logger.info(s"Fetching config from consul [${params.consulConfigPath}]...")
    consulManager.getFlag(params.consulConfigPath).map(ConfigFactory.parseString).map(conf => conf.resolve)
      .getOrElse(throw new RuntimeException(s"Can't fetch config from consul [$params]"))
  }

  private def fetchConfigFromPath(params: AppParams): Config = {
    val path = params.configPath
    logger.info(s"Fetching config from path [$path]...")

    lazy val from = if (isUrl(path)) Source.fromURL(path) else Source.fromFile(path)

    val confStr = for (resource <- managed(from)) yield resource.mkString
    confStr.tried.toOption.map(ConfigFactory.parseString).map(conf => conf.resolve)
      .getOrElse(throw new RuntimeException(s"Can't fetch config from path [$params]"))
  }


  private def resolveCkmsProperty(property: String): String = {
    property + "_resolved"
  }

  private def resolveCkmsProperties(conf: Config): Config = {
    val it = conf.entrySet().iterator()
    var updatedConf: Config = conf
    val pattern = Pattern.compile("\\$\\{(.*?)\\}")

    while (it.hasNext) {
      val property = it.next()
      val key = property.getKey
      val value = property.getValue
      val matcher = pattern.matcher(value.render())
      if (value.valueType().name().toLowerCase.contains("string") && matcher.find()){
        val resolvedCkmsValue = ConfigValueFactory.fromAnyRef(resolveCkmsProperty(matcher.group(1)))
        updatedConf = updatedConf.withValue(key, resolvedCkmsValue)
      }
    }

    updatedConf
  }

  private def fetchConfigFromResources: Config = {
    val configPrefix = EnvUtil.getEnvironment.map(_ + "/").getOrElse("")
    val configPath = s"${configPrefix}application.conf"
    logger.info(s"Fetching config from resources [$configPath]...")
    ConfigFactory.load(configPath)
  }

  private def isUrl(path: String): Boolean = {
    path.startsWith("http://") || path.startsWith("https://")
  }

  private def readCmdParams(args: Array[String]): Option[AppParams] = {
    val parser = new scopt.OptionParser[AppParams]("Bandarlog App") {
      override def errorOnUnknownArgument: Boolean = false

      head("Bandarlog App")

      opt[String]('c', "config-strategy")
        .text("config strategy [consul, path]")
        .validate { prop =>
          if (prop == "consul" || prop == "path" || prop == "resources") success
          else failure("Incorrect config strategy. Possible values: [consul, path, resources]")
        }
        .action { (b, params) => params.copy(configStrategy = b) }

      opt[String]('f', "config-path")
        .text("URL or local path to the config file")
        .action { (b, params) => params.copy(configPath = b) }

      opt[String]('h', "consul-host")
        .text("consul host")
        .action { (b, params) => params.copy(consulHost = b) }

      opt[String]('p', "consul-port")
        .text("consul port")
        .action { (b, params) => params.copy(consulPort = b.toInt) }

      opt[String]('t', "consul-path")
        .text("path to the config in the consul 'path/key'")
        .action { (b, params) => params.copy(consulConfigPath = b) }
    }
    parser.parse(args, AppParams())
  }

}
