package io.streamvault.pipeline

import io.streamvault.pipeline.config.AppConfig
import io.streamvault.pipeline.consumer.MediaUploadedConsumer
import io.streamvault.pipeline.infra.{FileDownloader, HealthServer}
import zio.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.consoleJsonLogger
import zio.metrics.connectors.prometheus

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
          MetricsConfig.defaultLayer,
          prometheus.prometheusLayer,
          prometheus.publisherLayer
        )
