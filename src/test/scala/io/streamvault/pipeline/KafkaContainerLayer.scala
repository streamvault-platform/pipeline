package io.streamvault.pipeline

import io.streamvault.pipeline.config.{AppConfig, KafkaConfig, KafkaTopicsConfig, ServerConfig}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import zio.*

object KafkaContainerLayer:

  val live: ZLayer[Any, Throwable, KafkaContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(
        ZIO.attemptBlocking {
          val c = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
          c.start()
          c
        }
      )(c => ZIO.attemptBlocking(c.stop()).ignore)
    )

  // Strips the "PLAINTEXT://" scheme that KafkaContainer.getBootstrapServers() includes.
  def bootstrapServers(container: KafkaContainer): String =
    container.getBootstrapServers.stripPrefix("PLAINTEXT://")

  def appConfig(container: KafkaContainer, consumerGroup: String = "test-pipeline"): AppConfig =
    AppConfig(
      kafka = KafkaConfig(
        bootstrapServers   = bootstrapServers(container),
        consumerGroup      = consumerGroup,
        topics             = KafkaTopicsConfig(
          mediaUploaded      = "media.uploaded",
          mediaUploadedDlq   = "media.uploaded.dlq",
          metadataReady      = "media.metadata-ready",
          transcoded         = "media.transcoded",
          watchSyncRequested = "watch.sync-requested"
        )
      ),
      server = ServerConfig(port = 8080, metricsInterval = 5.seconds)
    )
