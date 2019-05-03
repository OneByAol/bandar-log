package com.aol.one.dwh.infra.parser

import scala.util.parsing.combinator.RegexParsers

/**
  * Column parser
  *
  * Parses column=format pairs provided in config
  */
object ColumnParser extends RegexParsers {

  override def skipWhitespace: Boolean = false

  def pairSeparator: Parser[String] = "=".r
  def columnName: Parser[String] = """\w""".r
  def columnFormat: Parser[String] = """[\w\-\.\s\':,_]+""".r

  def pair: Parser[(String, String)] = columnName ~ pairSeparator ~ columnFormat ^^ {
    case c ~ _ ~ f => (c, f)
  }

  def parse(in: String): (String, String) = {
    parseAll(pair, in) match {
      case Success(result, _) => result
      case NoSuccess(msg, next) =>
        throw new Exception(s"Could not parse: $msg, $next")
    }
  }

  def parseList(in: List[String]): List[(String, String)] = in.map(parse)
}
