package com.ratoshniuk.scalaua

import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BlockingIO}
import com.github.pshirshov.izumi.logstage.api.IzLogger
import com.ratoshniuk.scalaua.YourService.{AdsPlarform, UserId}
import logstage.LogBIO
import scalaz.zio.duration.Duration
import scalaz.zio.{IO, Schedule, ZIO}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

class YourService
(
  logger: IzLogger
)(implicit blockingIO: BlockingIO[IO], BIOAsync: BIOAsync[IO])
{
  def batchProcessing(userId: UserId, companyId: AdsPlarform) : IO[Nothing, Unit] = {

    val processRule = Schedule.duration(Duration.fromScala(FiniteDuration.apply(10, "seconds")))

    val granularLogger = LogBIO.fromLogger[IO](logger.apply("userId" -> userId.string, "companyId" -> companyId.string))
    for {
      _             <- granularLogger.info("start processing batching")
      queue          = new ConcurrentLinkedQueue[List[String]]()
      _             <- BIOAsync.race(fetchOffset(queue, granularLogger) repeat  processRule)(processOffset(queue, granularLogger) repeat  processRule)
    } yield ()
  }

  def fetchOffset(safeTo: ConcurrentLinkedQueue[List[String]], logger: LogBIO[IO]) : IO[Nothing, Unit] = {
    IO.sleep(Duration.fromScala(FiniteDuration.apply(2, "seconds"))) *> logger.info(s"fetched ${2 * 1000 -> "rows"} from offset")
  }

  def processOffset(takeFrom: ConcurrentLinkedQueue[List[String]], logger: LogBIO[IO]) : IO[Nothing, Unit] = {
    IO.sleep(Duration.fromScala(FiniteDuration.apply(2, "seconds"))) *> logger.info(s"processed ${2 * 1000 -> "rows"} from offset")
  }
}

object YourService {
  case class UserId(string: String) extends AnyVal
  case class AdsPlarform(string: String) extends AnyVal
}
