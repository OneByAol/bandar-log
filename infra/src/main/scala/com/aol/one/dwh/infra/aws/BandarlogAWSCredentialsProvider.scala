package com.aol.one.dwh.infra.aws

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.aol.one.dwh.infra.config.AthenaConfig
import com.simba.athena.amazonaws.auth.BasicAWSCredentials


class BandarlogAWSCredentialsProvider(config: AthenaConfig) extends AWSCredentialsProvider {

  override def refresh(): Unit = {}

  override def getCredentials: AWSCredentials = {

    new BasicAWSCredentials(config.accessKey, config.secretKey)

  }
}
