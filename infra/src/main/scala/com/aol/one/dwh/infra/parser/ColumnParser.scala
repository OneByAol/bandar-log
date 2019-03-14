package com.aol.one.dwh.infra.parser

import scala.util.parsing.combinator.RegexParsers


object ColumnParser extends RegexParsers {

  override def skipWhitespace: Boolean = false

  def pairSep: Parser[String] = "=".r
  def column: Parser[String] = "\\w+".r
  def format: Parser[String] = "[\\w\\-\\.\\s\\':,]+".r

  def pair: Parser[(String, Option[String])] = column ~ opt(pairSep) ~ opt(format) ^^ {
    case c ~ _ ~ None => (c, None)
    case c ~ _ ~ Some(f) => (c, Some(f))
  }

  def parse(in: String): (String, Option[String]) = {
    parseAll(pair, in) match {
      case Success(result, _) => result
      case NoSuccess(msg, next) =>
        throw new Exception(s"Could not parse: $msg, $next")
    }
  }

  def parseList(in: List[String]): List[(String, Option[String])] = {
    in.map(parse)
  }
}