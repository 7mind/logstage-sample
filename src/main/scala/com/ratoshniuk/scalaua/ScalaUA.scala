package com.ratoshniuk.scalaua

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BIORunner}
import com.ratoshniuk.scalaua.AdReportService.{AdPlatform, UserId}
import scalaz.zio.IO
import cats.effect._
import scala.concurrent.ExecutionContext
import scala.util.Random
import logstage._

object ScalaUA extends App {

  lazy val blockingExecutor =  Executors.newCachedThreadPool()
  lazy val blockingEc =  ExecutionContext.fromExecutor(blockingExecutor)

  implicit lazy val csh = cats.effect.IO.contextShift(blockingEc)

  implicit lazy val bioRunner = BIORunner.createZIO(
    Executors.newFixedThreadPool(8).asInstanceOf[ThreadPoolExecutor]
    , blockingExecutor.asInstanceOf[ThreadPoolExecutor]
  )

  val textSink = ConsoleSink.text(colored = true)
  val jsonSink = new DBJsonSink(new PostgresConnector(blockingEc))

  val sinks = List(textSink, jsonSink)

  val logger: IzLogger = IzLogger.apply(Trace, sinks)

  val service = new AdReportService(logger)

  val adPlatforms = List(AdPlatform.BadGuys, AdPlatform.GoodFellows)

  val userCount = 10
  val eff = IO.foreachParN(5)(1 to userCount) {
    id =>
      service.pullReports(UserId(id), Random.shuffle(adPlatforms).head)
  }.void

  bioRunner.unsafeRun(eff)

  sys.exit(0)
}
