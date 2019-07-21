package com.ratoshniuk.logstage

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.github.pshirshov.izumi.functional.bio.impl.BIOAsyncZio
import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BIORunner}
import com.ratoshniuk.logstage.AdReportService.{AdPlatform, UserId}
import zio.{DefaultRuntime, IO}
import zio.clock.Clock

import scala.concurrent.ExecutionContext
import scala.util.Random


object Runner extends App with EffectRuntime  {
val service = new AdReportService

  val adPlatforms = List(AdPlatform.BadGuys, AdPlatform.GoodFellows)

  val userCount = 10
  val eff = zio.ZIO.foreachParN(5)(1 to userCount) {
    id => service.pullReports(UserId(id), Random.shuffle(adPlatforms).head)
  }.unit

  zioRunner.unsafeRun(eff)

  sys.exit(0)
}

trait EffectRuntime {

  val cpuPool : ThreadPoolExecutor = {
    Executors.newFixedThreadPool(8).asInstanceOf[ThreadPoolExecutor]
  }

  val zioPool : ThreadPoolExecutor = {
    val cores = Runtime.getRuntime.availableProcessors.max(2)
    new ThreadPoolExecutor(cores, cores, 0L, TimeUnit.MILLISECONDS,  new LinkedBlockingQueue[Runnable])
  }

  val blockingIO : ExecutionContext = ExecutionContext.fromExecutorService(zioPool): ExecutionContext

  val zioRunner = new DefaultRuntime {}

  implicit val async: BIOAsync[IO] =  new BIOAsyncZio(Clock.Live)
}
