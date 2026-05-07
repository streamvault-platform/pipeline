package io.streamvault.pipeline.config

import zio.*
import zio.config.magnolia.deriveConfig

final case class KafkaTopicsConfig(
  mediaUploaded:     String,
  mediaUploadedDlq:  String,
  metadataReady:     String,
  transcoded:        String,
  watchSyncRequested: String
)

final case class KafkaConfig(
  bootstrapServers: String,
  consumerGroup:    String,
  topics:           KafkaTopicsConfig
)

final case class ServerConfig(port: Int, metricsInterval: Duration)

final case class AppConfig(kafka: KafkaConfig, server: ServerConfig)

object AppConfig:
  val layer: ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(ZIO.config(deriveConfig[AppConfig]))
