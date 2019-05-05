package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.infra.config._
import com.aol.one.dwh.infra.parser.ColumnParser
import org.scalatest.FunSuite

class ColumnParserTest extends FunSuite {

  test("Parse column and its format from bandarlog config") {
    val columns = List("year=yyyy", "month=MM", "day=dd")
    val expectedResult = List(("year", "yyyy"), ("month", "MM"), ("day", "dd"))

    val actualResult = ColumnParser.parseList(columns)

    assert(expectedResult equals actualResult)
  }
}
