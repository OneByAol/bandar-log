package com.aol.one.dwh.infra.parser

import java.text.{DateFormat, SimpleDateFormat}
import java.util.TimeZone

import com.aol.one.dwh.infra.util.{ExceptionPrinter, LogTrait}

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object StringToTimestampParser extends LogTrait with ExceptionPrinter {

  def parse(value: String, format: String): Option[Long] = {

    Try {
      val dateFormat: DateFormat = new SimpleDateFormat(format)
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

      val date = dateFormat.parse(value)
      date.getTime
    }.recoverWith {
        case NonFatal(e) =>
          logger.error(s"Could not parse value:[$value] using format:[$format]. Catching exception {}", e.getStringStackTrace)
          Failure(e)
    }.toOption

  }
}
