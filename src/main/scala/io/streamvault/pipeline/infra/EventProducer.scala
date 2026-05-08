package io.streamvault.pipeline.infra

import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords}
import io.streamvault.pipeline.config.KafkaTopicsConfig
import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent}
import zio.*
import zio.json.*

final class EventProducer(
  raw: KafkaProducer[Task, String, String],
  topics: KafkaTopicsConfig,
  bootstrapServers: String
) extends EventSink:
  def produceMetadataReady(event: MetadataReadyEvent): Task[Unit] =
    ZIO.logDebug(s"action=kafka_publish topic=${topics.metadataReady} bootstrap=$bootstrapServers key=${event.trackId} payload=${event.toJson}") *>
      produce(topics.metadataReady, event.trackId.toString, event.toJson)

  def produceTranscoded(event: TranscodedEvent): Task[Unit] =
    ZIO.logDebug(s"action=kafka_publish topic=${topics.transcoded} bootstrap=$bootstrapServers key=${event.trackId} payload=${event.toJson}") *>
      produce(topics.transcoded, event.trackId.toString, event.toJson)

  def sendToDlq(key: String, value: String): Task[Unit] =
    ZIO.logWarning(s"action=kafka_publish_dlq topic=${topics.mediaUploadedDlq} bootstrap=$bootstrapServers key=$key payload=$value") *>
      produce(topics.mediaUploadedDlq, key, value)

  private def produce(topic: String, key: String, value: String): Task[Unit] =
    raw.produce(ProducerRecords.one(ProducerRecord(topic, key, value))).flatten.unit
      .tap(_ => ZIO.logInfo(s"action=kafka_publish_ok topic=$topic bootstrap=$bootstrapServers key=$key"))
