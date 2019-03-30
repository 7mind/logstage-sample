package com.ratoshniuk.scalaua

import com.github.pshirshov.izumi.functional.bio.BIOAsync
import com.github.pshirshov.izumi.logstage.api.IzLogger
import com.ratoshniuk.scalaua.AdReportService.AdPlatform.{BadGuys, GoodFellows}
import com.ratoshniuk.scalaua.AdReportService.{AdPlatform, UserId}
import logstage.{LogBIO, LogstageZIO}
import scalaz.zio.duration.Duration
import scalaz.zio.{IO, Schedule}

import scala.concurrent.duration._
import scala.util.Random

class AdReportService(logger: IzLogger)(implicit AsyncIO: BIOAsync[IO])
{
  def pullReports(userId: UserId, adPlatform: AdPlatform) : IO[Nothing, Unit] = {
    val granularLog = LogstageZIO.withFiberId(
      logger("userId" -> userId.value, "ad" -> adPlatform.toString)
    )
    for {
      _        <- granularLog.info(s"start processing batching")
      readIO    = readFromNetwork(userId, adPlatform, granularLog) repeat  processRule
      writeIO   = putIntoS3(adPlatform, granularLog) repeat  processRule
      _        <- AsyncIO.race(readIO)(writeIO)
      _        <- granularLog.info(s"batch processing completed")
    } yield ()
  }

  def readFromNetwork(userId: UserId, adPlatform: AdPlatform, log: LogBIO[IO]) : IO[Nothing, Unit] = {
    adPlatform match {
      case BadGuys => AsyncIO.sleep(0.3.seconds) *> log.error(s"Rate limit exceeded")
      case _       => AsyncIO.sleep(1.second)    *> log.info(s"Received ${2 * 1000 -> "rows"}")
    }
  }

  def putIntoS3(adPlatform: AdPlatform, log: LogBIO[IO]) : IO[Nothing, Unit] = {
    adPlatform match {
      case BadGuys => AsyncIO.sleep(0.3.seconds) *> log.warn(s"Uploading to S3 Skipped")
      case _       => AsyncIO.sleep(1.second)    *> log.info(s"Received ${2 * 1000 -> "rows"}")
    }
  }

  private val processRule = Schedule.duration(Duration.fromScala(5.seconds))
}

object AdReportService {

  case class UserId(value: Int) extends AnyVal

  sealed trait AdPlatform

  object AdPlatform {
    case object BadGuys extends AdPlatform {
      override def toString: String = "BadGuys"
    }
    case object GoodFellows extends AdPlatform {
      override def toString: String = "GoodFellows"
    }

    case object AnotherGoodOnes extends AdPlatform {
      override def toString: String = "GoodFellows"
    }

    def random: AdPlatform = Random.shuffle(Set(BadGuys, GoodFellows, AnotherGoodOnes)).head
  }


}
