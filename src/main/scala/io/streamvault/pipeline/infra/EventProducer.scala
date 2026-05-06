package io.streamvault.pipeline.infra

import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords}
import io.streamvault.pipeline.config.KafkaTopicsConfig
import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent}
import zio.*
import zio.json.*

final class EventProducer(
  raw: KafkaProducer[Task, String, String],
  topics: KafkaTopicsConfig
):
  def produceMetadataReady(event: MetadataReadyEvent): Task[Unit] =
    produce(topics.metadataReady, event.trackId.toString, event.toJson)

  def produceTranscoded(event: TranscodedEvent): Task[Unit] =
    produce(topics.transcoded, event.trackId.toString, event.toJson)

  def sendToDlq(key: String, value: String): Task[Unit] =
    produce(topics.mediaUploadedDlq, key, value)

  private def produce(topic: String, key: String, value: String): Task[Unit] =
    raw.produce(ProducerRecords.one(ProducerRecord(topic, key, value))).flatten.unit
