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
    val q = Update[(Long, String)]("""INSERT INTO "public"."structured_logs"("timestamp", "log") VALUES(TO_TIMESTAMP(?::double precision / 1000), jsonb(?))""".stripMargin).updateMany(NonEmptyList.fromListUnsafe(entries))
    pgConnector.query(q).void
  }
}

import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.syntax.connectionio._
import logstage.IzLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class PostgresConnector[F[+_, +_]: BIO: BIOAsync]
(
  blockingIOExecutionContext: ExecutionContext
)(implicit cs: ContextShift[cats.effect.IO])    {

  import scala.concurrent.duration._

  def query[T](query: ConnectionIO[T]): F[Nothing, T] = {
    for {
      res <- {
        BIO[F].syncThrowable(query.transact(mkTransactor).unsafeRunSync())
          .catchAll(f => BIO[F].terminate(f))
      }
    } yield res
  }

  private[this] lazy val mkTransactor: HikariTransactor[cats.effect.IO] = {
    val ds = new HikariDataSource(cfg)
    HikariTransactor.apply[cats.effect.IO](ds, blockingIOExecutionContext, blockingIOExecutionContext)
  }

  private val cfg : HikariConfig = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:postgresql://localhost/postgres")
    config.setUsername("postgres")
    config.setPassword("postgres")
    config.setDriverClassName("org.postgresql.Driver")
    config
  }

}