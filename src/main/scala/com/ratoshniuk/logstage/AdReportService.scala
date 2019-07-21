package com.ratoshniuk.logstage

import com.github.pshirshov.izumi.functional.bio.BIOAsync
import com.ratoshniuk.logstage.AdReportService.{AdPlatform, UserId}
import zio.duration.Duration.Finite
import zio.{IO, Schedule}

import scala.concurrent.duration._
import scala.util.Random

class AdReportService(implicit async: BIOAsync[IO])
{
  def pullReports(userId: UserId, adPlatform: AdPlatform) : IO[Nothing, Unit] = {
    for {
      _        <- IO.succeed(println(s"start pulling for $userId amd $adPlatform"))
      readIO    = (readFromNetwork(userId, adPlatform) repeat  processRule).unit
      writeIO   = (putIntoS3(adPlatform) repeat  processRule).unit
      _        <- async.parTraverseN(2)(List(readIO, writeIO))(_ => IO.unit)
    } yield ()
  }
  def readFromNetwork(userId: UserId, adPlatform: AdPlatform) :  IO[Nothing, Unit] = {
    async.sleep(5.seconds).unit
  }

  def putIntoS3(adPlatform: AdPlatform) : IO[Nothing, Unit] = {
    async.sleep(10.seconds).unit
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
