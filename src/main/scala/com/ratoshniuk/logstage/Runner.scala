package com.ratoshniuk.logstage

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.github.pshirshov.izumi.functional.bio.BIOAsync
import com.github.pshirshov.izumi.functional.bio.impl.BIOAsyncZio
import com.github.pshirshov.izumi.logstage.api.IzLogger
import com.github.pshirshov.izumi.logstage.api.Log.Level.Trace
import com.github.pshirshov.izumi.logstage.sink.ConsoleSink
import com.ratoshniuk.logstage.AdReportService.{AdPlatform, UserId}
import zio.clock.Clock
import zio.{DefaultRuntime, IO}

import scala.concurrent.ExecutionContext
import scala.util.Random


object Runner extends App with EffectRuntime with WithLogger {
  val service = new AdReportService(logger)

  val adPlatforms = List(AdPlatform.BadGuys, AdPlatform.GoodFellows)

  val userCount = 10
  val eff = zio.ZIO.foreachParN(5)(1 to userCount) {
    id => service.pullReports(UserId(id), Random.shuffle(adPlatforms).head)
  }.unit

  logger.info("start running app")

  zioRunner.unsafeRun(eff)

  sys.exit(0)
}


trait WithLogger {
  this: EffectRuntime =>

  val textSink = ConsoleSink.text(colored = true)

  val dbSink: DBJsonSink = new DBJsonSink(blockingIO)

  val sinks = List(textSink, dbSink)

  val logger: IzLogger = IzLogger.apply(Trace, sinks)
}


trait EffectRuntime {

  val cpuPool: ThreadPoolExecutor = {
    Executors.newFixedThreadPool(8).asInstanceOf[ThreadPoolExecutor]
  }

  val zioPool: ThreadPoolExecutor = {
    val cores = Runtime.getRuntime.availableProcessors.max(2)
    new ThreadPoolExecutor(cores, cores, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
  }

  val blockingIO: ExecutionContext = ExecutionContext.fromExecutorService(zioPool): ExecutionContext

  implicit val zioRunner: DefaultRuntime = new DefaultRuntime {}

  implicit val async: BIOAsync[IO] = new BIOAsyncZio(Clock.Live)
}