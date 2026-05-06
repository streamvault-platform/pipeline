package io.streamvault.pipeline

import io.streamvault.pipeline.config.AppConfig
import io.streamvault.pipeline.consumer.MediaUploadedConsumer
import zio.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.logging.consoleJsonLogger

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    (Runtime.removeDefaultLoggers >>> consoleJsonLogger()) ++
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath())

  override def run: ZIO[Any, Any, Any] =
    ZIO.logInfo("Streamvault pipeline starting") *>
      MediaUploadedConsumer.consume
        .provide(
          AppConfig.layer,
          MediaUploadedConsumer.live
        )
