package com.aol.one.dwh.infra.parser

import com.aol.one.dwh.infra.config._
import scala.util.parsing.combinator.RegexParsers


object ColumnParser extends RegexParsers {

  override def skipWhitespace: Boolean = false

  def pairSep: Parser[String] = "=".r
  def column: Parser[String] = "\\w+".r
  def format: Parser[String] = "[\\w\\-\\.\\s\\':,]+".r

  def pair: Parser[DatetimePatition] = column ~ pairSep ~ format ^^ {
    case c ~ _ ~ f => DatetimePatition(c, f)
  }

  def parse(in: String): DatetimePatition = {
    parseAll(pair, in) match {
      case Success(result, _) => result
      case NoSuccess(msg, next) =>
        throw new Exception(s"Could not parse: $msg, $next")
    }
  }

  def parseList(in: List[String]): List[DatetimePatition] = {
    in.map(parse)
  }
}
