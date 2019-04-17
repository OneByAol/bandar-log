package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.infra.config.Partition
import com.aol.one.dwh.infra.parser.ColumnParser
import org.scalatest.FunSuite

class ColumnParserTest extends FunSuite {

  val columns = List("year=yyyy", "month=MM", "day=dd")

  test("Parse column and format from bandarlog config") {

    val expectedResult = List(Partition("year", "yyyy"), Partition("month", "MM"), Partition("day", "dd"))
    val actualResult = ColumnParser.parseList(columns)
    assert(expectedResult equals actualResult)

  }



}
