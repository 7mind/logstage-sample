package com.ratoshniuk.logstage

import com.github.pshirshov.izumi.functional.bio.BIOAsync
import com.github.pshirshov.izumi.logstage.api.IzLogger
import com.ratoshniuk.logstage.AdReportService.AdPlatform.BadGuys
import com.ratoshniuk.logstage.AdReportService.{AdPlatform, UserId}
import zio.duration.Duration.Finite
import zio.{IO, Schedule}

import scala.concurrent.duration._
import scala.util.Random
import com.ratoshniuk.logstage.AdReportService.AdPlatform.BadGuys
import logstage.LogBIO
import zio.clock.Clock
import zio.{IO, Schedule, ZIO}

class AdReportService(logger: IzLogger)(implicit async: BIOAsync[IO])
{
  def pullReports(userId: UserId, adPlatform: AdPlatform) : ZIO[Any with Clock, Nothing, Unit] = {
    val granularLog = LogBIO.fromLogger[IO](
      logger(
        "userId" -> userId.value,
        "ad" -> adPlatform.toString
      )
    )
    for {
      _        <- granularLog.info(s"start processing batching")
      readIO    = (readFromNetwork(userId, adPlatform, granularLog) repeat  processRule).unit
      writeIO   = (putIntoS3(adPlatform, granularLog) repeat  processRule).unit
      _        <- zio.ZIO.raceAll(readIO, Seq(writeIO))
      _        <- granularLog.info(s"batch processing completed")
    } yield ()
  }
  def readFromNetwork(userId: UserId, adPlatform: AdPlatform, log: LogBIO[IO]) :  IO[Nothing, Unit] = {
    adPlatform match {
      case BadGuys => async.sleep(0.3.seconds) *> log.error(s"Rate limit exceeded")
      case _       => async.sleep(1.second)    *> log.info(s"Received ${2 * 1000 -> "rows"}")
    }
  }

  def putIntoS3(adPlatform: AdPlatform, log: LogBIO[IO]) : IO[Nothing, Unit] = {
    adPlatform match {
      case BadGuys => async.sleep(0.3.seconds) *> log.warn(s"Uploading to S3 Skipped")
      case _       => async.sleep(1.second)    *> log.info(s"Received ${2 * 1000 -> "rows"}")
    }
  }

  private val processRule = Schedule.spaced(Finite.apply(5.seconds.toNanos))
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
