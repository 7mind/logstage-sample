package com.ratoshniuk.scalaua

import cats.effect.ContextShift
import com.github.pshirshov.izumi.functional.bio.{BIO, BIOAsync}
import BIO._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
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
    BIO[F].syncThrowable(
      query.transact(mkTransactor).unsafeRunSync()
    ).catchAll(BIO[F].terminate(_))
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