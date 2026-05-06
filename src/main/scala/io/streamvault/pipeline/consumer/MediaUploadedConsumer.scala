package io.streamvault.pipeline.consumer

import io.streamvault.pipeline.config.AppConfig
import zio.*

trait MediaUploadedConsumer:
  def consume: Task[Unit]

object MediaUploadedConsumer:
  val live: ZLayer[AppConfig, Nothing, MediaUploadedConsumer] =
    ZLayer.fromFunction { (cfg: AppConfig) =>
      new MediaUploadedConsumer:
        def consume: Task[Unit] =
          ZIO.logInfo(s"Starting consumer on topic: ${cfg.kafka.topics.mediaUploaded}") *>
            ZIO.never
    }

  def consume: ZIO[MediaUploadedConsumer, Throwable, Unit] =
    ZIO.serviceWithZIO[MediaUploadedConsumer](_.consume)
