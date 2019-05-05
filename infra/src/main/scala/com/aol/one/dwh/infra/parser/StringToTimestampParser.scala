package com.aol.one.dwh.infra.parser

import java.text.{DateFormat, SimpleDateFormat}
import java.util.TimeZone

import com.aol.one.dwh.infra.util.{ExceptionPrinter, LogTrait}

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/**
  * String to timestamp parser
  *
  * Parses date/time string values of partitions to produce the number of milliseconds since January 1, 1970, 00:00:00 GMT
  * represented by this date
  */
object StringToTimestampParser extends LogTrait with ExceptionPrinter {

  def parse(value: String, format: String): Option[Long] = {

    Try {
      val dateFormat: DateFormat = new SimpleDateFormat(format)
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
      dateFormat.parse(value).getTime
    }.recoverWith {
        case NonFatal(e) =>
          logger.error(s"Could not parse value:[$value] using format:[$format]. Catching exception {}", e.getStringStackTrace)
          Failure(e)
    }.toOption
  }
}
