package io.streamvault.pipeline.infra

import io.streamvault.pipeline.config.AppConfig
import zio.*
import zio.http.*
import zio.metrics.connectors.prometheus.PrometheusPublisher

object HealthServer:

  val routes: Routes[PrometheusPublisher, Nothing] = Routes(
    Method.GET / "health" -> Handler.fromZIO(
      ZIO.succeed(Response.json("""{"status":"ok"}"""))
    ),
    Method.GET / "metrics" -> Handler.fromZIO(
      ZIO.serviceWithZIO[PrometheusPublisher](_.get).map(Response.text(_))
    )
  )

  val serverLayer: ZLayer[AppConfig, Throwable, Server] =
    ZLayer(
      ZIO.serviceWith[AppConfig](cfg =>
        Server.Config.default.port(cfg.server.port)
      )
    ) >>>
      Server.live
