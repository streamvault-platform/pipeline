package io.streamvault.pipeline.config

import zio.*
import zio.config.magnolia.deriveConfig

final case class KafkaTopicsConfig(
  mediaUploaded: String,
  watchSyncRequested: String
)

final case class KafkaConfig(
  bootstrapServers: String,
  consumerGroup: String,
  topics: KafkaTopicsConfig
)

final case class AppConfig(kafka: KafkaConfig)

object AppConfig:
  val layer: ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(ZIO.config(deriveConfig[AppConfig]))
