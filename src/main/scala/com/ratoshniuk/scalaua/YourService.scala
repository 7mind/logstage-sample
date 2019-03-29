package com.ratoshniuk.scalaua

import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BlockingIO}
import com.github.pshirshov.izumi.logstage.api.IzLogger
import com.ratoshniuk.scalaua.YourService.{AdsPlatform, UserId}
import logstage.LogBIO
import scalaz.zio.duration.Duration
import scalaz.zio.{IO, Schedule, ZIO}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

class YourService
(
  logger: IzLogger
)(implicit BIOAsync: BIOAsync[IO])
{
  def batchProcessing(userId: UserId, companyId: AdsPlatform) : IO[Nothing, Unit] = {
    val processRule = Schedule.duration(Duration.fromScala(5.seconds))
    val granularLogger = LogBIO.fromLogger[IO](
      logger(
        "userId" -> userId.string,
        "companyId" -> companyId.string
      )
    )
    for {
      _    <- granularLogger.info("start processing batching")
      _    <- BIOAsync.race(fetchOffset(granularLogger) repeat  processRule)(processOffset(granularLogger) repeat  processRule)
    } yield ()
  }

  def fetchOffset(logger: LogBIO[IO]) : IO[Nothing, Unit] = {
    BIOAsync.sleep(1.second) *> logger.info(s"fetched ${2 * 1000 -> "rows"} from offset")
  }

  def processOffset(logger: LogBIO[IO]) : IO[Nothing, Unit] = {
    BIOAsync.sleep(1.second) *> logger.info(s"processed ${2 * 1000 -> "rows"} from offset")
  }
}

object YourService {
  case class UserId(string: String) extends AnyVal
  case class AdsPlatform(string: String) extends AnyVal
}
