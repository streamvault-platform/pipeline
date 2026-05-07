package io.streamvault.pipeline

import io.streamvault.pipeline.config.AppConfig
import io.streamvault.pipeline.consumer.MediaUploadedConsumer
import io.streamvault.pipeline.infra.{FileDownloader, HealthServer}
import zio.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.consoleJsonLogger
import zio.metrics.connectors.{MetricsConfig, prometheus}

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    (Runtime.removeDefaultLoggers >>> consoleJsonLogger()) ++
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath())

  override def run: ZIO[Any, Any, Any] =
    ZIO.logInfo("Streamvault pipeline starting") *>
      (MediaUploadedConsumer.consume <&> Server.serve(HealthServer.routes)).unit
        .provide(
          AppConfig.layer,
          FileDownloader.live,
          MediaUploadedConsumer.live,
          HealthServer.serverLayer,
          ZLayer.fromZIO(ZIO.serviceWith[AppConfig](cfg => MetricsConfig(cfg.server.metricsInterval))),
          prometheus.prometheusLayer,
          prometheus.publisherLayer
        )
