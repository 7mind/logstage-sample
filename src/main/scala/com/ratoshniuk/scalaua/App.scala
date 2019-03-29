package com.ratoshniuk.scalaua

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BIORunner}
import com.ratoshniuk.scalaua.YourService.{AdsPlatform, UserId}
import scalaz.zio.IO

import scala.util.Random

object ScalaUA extends App {

  import logstage._

  implicit val bioRunner = BIORunner.createZIO(
    Executors.newFixedThreadPool(8).asInstanceOf[ThreadPoolExecutor]
    , Executors.newCachedThreadPool().asInstanceOf[ThreadPoolExecutor]
  )

//  val textSink = ConsoleSink.text(colored = true)
  val jsonSink = new DBJsonSink()

//  val sinks = List(textSink)
  val sinks = List(jsonSink)

  val logger: IzLogger = IzLogger.apply(Trace, sinks)

  val service = new YourService(logger)



  val adPlatforms = List("Adwords", "Google", "IronSource")

  val userCount = 10
  val eff = BIOAsync[IO].parTraverseN(5)((1 to userCount) map (i => s"user-$i") ) {
    user =>
      service.batchProcessing(UserId(user), AdsPlatform(Random.shuffle(adPlatforms).head))
  }.void

  bioRunner.unsafeRun(eff)

  sys.exit(0)
}
