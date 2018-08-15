/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.metrics.AtomicValue
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class SqlLagProviderTest extends FunSuite with MockitoSugar {

  private val fromProvider = mock[SqlTimestampProvider]
  private val toProvider = mock[SqlTimestampProvider]
  private val toGlueProvider = mock[GlueTimestampProvider]
  private val lagProvider1 = new SqlLagProvider(fromProvider, toProvider)
  private val lagProvider2 = new SqlLagProvider(fromProvider, toGlueProvider)

  test("check lag between from and to providers") {
    val fromValue = AtomicValue(Some(7L))
    val toValue = AtomicValue(Some(4L))
    val toGlueValue = AtomicValue(Some(6L))

    when(fromProvider.provide()).thenReturn(fromValue)
    when(toProvider.provide()).thenReturn(toValue)
    when(toGlueProvider.provide()).thenReturn(toGlueValue)

    val lag1 = lagProvider1.provide()
    val lag2 = lagProvider2.provide()

    assert(lag1.getValue.nonEmpty)
    assert(lag1.getValue.get == 3)
    assert(lag2.getValue.nonEmpty)
    assert(lag2.getValue.get == 1)
  }

  test("return none if 'from provider' value is none") {
    val toValue = AtomicValue(Some(4L))

    when(fromProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toProvider.provide()).thenReturn(toValue)

    val lag = lagProvider1.provide()

    assert(lag.getValue.isEmpty)
  }

  test("return none if 'to provider' value is none") {
    val fromValue = AtomicValue(Some(7L))

    when(fromProvider.provide()).thenReturn(fromValue)
    when(toProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toGlueProvider.provide()).thenReturn(AtomicValue[Long](None))

    val lag1 = lagProvider1.provide()
    val lag2 = lagProvider2.provide()

    assert(lag1.getValue.isEmpty)
    assert(lag2.getValue.isEmpty)
  }

  test("return none if both providers values is none") {
    when(fromProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toGlueProvider.provide()).thenReturn(AtomicValue[Long](None))

    val lag1 = lagProvider1.provide()
    val lag2 = lagProvider2.provide()

    assert(lag1.getValue.isEmpty)
    assert(lag2.getValue.isEmpty)
  }
}
