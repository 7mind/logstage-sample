package com.ratoshniuk.scalaua

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentLinkedQueue}

import cats.effect.ContextShift
import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync, BIORunner, BlockingIO}
import com.github.pshirshov.izumi.logstage.api.Log
import BIO._
import cats.data.NonEmptyList
import com.github.pshirshov.izumi.logstage.api.logger.LogSink
import com.github.pshirshov.izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.util.update.Update
import logstage.circe.LogstageCirce
import scalaz.zio.IO

import scala.collection.mutable.ListBuffer

class DBJsonSink
(
  pgConnector: PostgresConnector[IO]
)(implicit BIORunner: BIORunner[IO]) extends LogSink {

  val policy = new LogstageCirceRenderingPolicy(false)

  val limit = 10

  val msgsAtomic = new AtomicReference[ListBuffer[(Long, String)]](ListBuffer.empty[(Long, String)])

  override def flush(e: Log.Entry): Unit = {
    val jsonified = policy.render(e)
    val msgs = msgsAtomic.get()
    if (msgs.size == limit) {
      val beforeSend = msgs.takeRight(limit).toList
      BIORunner.unsafeRun(insertIntoDB(beforeSend))
      msgsAtomic.set(msgs -- beforeSend)
    } else {
      msgs += Tuple2(e.context.dynamic.tsMillis, jsonified)
      msgsAtomic.set(msgs)
    }
  }

  private def insertIntoDB(entries : List[(Long, String)]) : IO[Nothing, Unit] = {
    import doobie.implicits._
    val sqlQuery =
      """
        |INSERT INTO "public"."structured_logs"
        | ("timestamp", "log")
        | VALUES(TO_TIMESTAMP(?::double precision / 1000), jsonb(?))
        | """.stripMargin
    val q = Update[(Long, String)](sqlQuery).updateMany(NonEmptyList.fromListUnsafe(entries))
    pgConnector.query(q).void
  }
}

