package com.aol.one.dwh.bandarlog

import java.text.{DateFormat, SimpleDateFormat}

import com.aol.one.dwh.infra.parser.StringToTimestampParser
import org.scalatest.FunSuite

class StringToTimestampParserTest extends FunSuite {

  val columnValue = "2018:10:11"
  val format = "yyyy:MM:dd"

  test("Parse partition date value to timestamp") {

    val expectedResult: Option[Long] = Some(1539205200000L)

    val actualResult: Option[Long] = StringToTimestampParser.parse(columnValue, format)
    assert(expectedResult equals actualResult)

  }

}
