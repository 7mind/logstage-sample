package com.ratoshniuk.logstage

import java.util.concurrent.atomic.AtomicReference

import cats.data.NonEmptyList
import com.github.pshirshov.izumi.logstage.api.Log
import com.github.pshirshov.izumi.logstage.api.logger.LogSink
import com.github.pshirshov.izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.syntax.connectionio._
import doobie.util.update.Update
import zio.interop.catz._
import zio.{DefaultRuntime, IO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class DBJsonSink
(ec: ExecutionContext)(implicit runtime: DefaultRuntime) extends LogSink {

  val policy = new LogstageCirceRenderingPolicy(false)

  val limit = 10

  val msgsAtomic = new AtomicReference[ListBuffer[(Long, String)]](ListBuffer.empty[(Long, String)])

  override def flush(e: Log.Entry): Unit = {
    val jsonified = policy.render(e)
    val msgs = msgsAtomic.get()
    if (msgs.size == limit) {
      val beforeSend = msgs.takeRight(limit).toList
      runtime.unsafeRun(insertIntoDB(beforeSend))
      msgsAtomic.set(msgs -- beforeSend)
    } else {
      msgs += Tuple2(e.context.dynamic.tsMillis, jsonified)
      msgsAtomic.set(msgs)
    }
  }

  private def insertIntoDB(entries : List[(Long, String)]) : IO[Nothing, Unit] = {
    val sqlQuery =
      """
        |INSERT INTO "public"."logstage_out"
        | ("at", "payload")
        | VALUES(TO_TIMESTAMP(?::double precision / 1000), jsonb(?))
        | """.stripMargin
    val q = Update[(Long, String)](sqlQuery).updateMany(NonEmptyList.fromListUnsafe(entries))
    query(q).unit
  }


  def query[T](query: ConnectionIO[T]): IO[Nothing, T] = {
    query.transact[zio.Task](mkTransactor).catchAll {
      thr =>
        println(thr.getMessage)
        zio.IO.interrupt //we will ignore errors there
    }
  }

  private[this] lazy val mkTransactor: HikariTransactor[IO[Throwable, ?]] = {
    val ds = new HikariDataSource(cfg)
    HikariTransactor.apply[IO[Throwable, ?]](ds, ec, ec)
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
