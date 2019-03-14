package com.aol.one.dwh.infra.parser

import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.Date

import com.aol.one.dwh.infra.util.LogTrait

object FormatParser extends LogTrait {

  def parse(value: String, format: String): Option[Long] = {

    val dateFormat: DateFormat = new SimpleDateFormat(format)

      try {
        val date = dateFormat.parse(value)
        Some(date.getTime)
      } catch {
        case ex: ParseException =>
          logger.error(s"Could not parse value $value. Returning None.")
          None
      }
  }

}
