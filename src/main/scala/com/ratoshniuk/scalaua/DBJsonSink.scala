package com.ratoshniuk.scalaua

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentLinkedQueue}

import com.github.pshirshov.izumi.functional.bio.{BIORunner, BlockingIO}
import com.github.pshirshov.izumi.logstage.api.Log
import com.github.pshirshov.izumi.logstage.api.logger.LogSink
import com.github.pshirshov.izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import logstage.circe.LogstageCirce
import scalaz.zio.IO

import scala.collection.mutable.ListBuffer

class DBJsonSink(
//                  blockingIO: BlockingIO[IO]
                )(implicit BIORunner: BIORunner[IO]) extends LogSink {

  val policy = new LogstageCirceRenderingPolicy(false)

  val limit = 10

  val msgsAtomic = new AtomicReference[ListBuffer[String]](ListBuffer.empty[String])

  override def flush(e: Log.Entry): Unit = {
    val jsonified = policy.render(e)
    val msgs = msgsAtomic.get()
    if (msgs.size == limit) {
      val beforeSend = msgs.takeRight(limit).toList
      msgsAtomic.set(msgs -- beforeSend)
//      BIORunner.unsafeRun()
    } else {
      msgs += jsonified
      msgsAtomic.set(msgs)
    }
  }
}