package io.streamvault.pipeline.infra

import fs2.kafka.*
import io.streamvault.pipeline.KafkaContainerLayer
import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent}
import org.testcontainers.containers.KafkaContainer
import zio.*
import zio.interop.catz.*
import zio.json.*
import zio.test.*
import zio.test.TestAspect.*

import java.util.UUID

object EventProducerSpec extends ZIOSpecDefault:

  // Sends one message via an EventProducer backed by a real KafkaProducer.
  private def withProducer[A](container: KafkaContainer)(f: EventProducer => Task[A]): Task[A] =
    val bs     = KafkaContainerLayer.bootstrapServers(container)
    val cfg    = KafkaContainerLayer.appConfig(container).kafka.topics
    val settings = ProducerSettings[Task, String, String].withBootstrapServers(bs)
    KafkaProducer
      .stream(settings)
      .evalMap(raw => f(EventProducer(raw, cfg)))
      .take(1)
      .compile
      .lastOrError

  // Consumes one record value from a topic; each call uses a fresh consumer group.
  private def consumeOne(container: KafkaContainer, topic: String): Task[String] =
    val bs = KafkaContainerLayer.bootstrapServers(container)
    KafkaConsumer
      .stream(
        ConsumerSettings[Task, String, String]
          .withBootstrapServers(bs)
          .withGroupId(s"test-${UUID.randomUUID()}")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withEnableAutoCommit(true)
      )
      .evalTap(_.subscribeTo(topic))
      .flatMap(_.stream)
      .take(1)
      .map(_.record.value)
      .compile
      .lastOrError

  def spec = suite("EventProducer")(

    test("produceMetadataReady sends a decodable record to the metadata-ready topic") {
      val trackId = UUID.randomUUID()
      val event = MetadataReadyEvent(
        trackId = trackId, title = Some("Hello"), artist = Some("World"),
        album = None, year = Some(2024), trackNumber = Some(1),
        discNumber = None, durationMs = Some(180000), genre = None
      )
      for
        container <- ZIO.service[KafkaContainer]
        _         <- withProducer(container)(_.produceMetadataReady(event))
        raw       <- consumeOne(container, "media.metadata-ready")
        decoded   <- ZIO.fromEither(raw.fromJson[MetadataReadyEvent])
      yield assertTrue(decoded.trackId == trackId) &&
            assertTrue(decoded.title == Some("Hello"))
    },

    test("produceTranscoded sends a decodable record to the transcoded topic") {
      val trackId = UUID.randomUUID()
      val event = TranscodedEvent(trackId, "path/file.aac", "audio/aac", 55555L)
      for
        container <- ZIO.service[KafkaContainer]
        _         <- withProducer(container)(_.produceTranscoded(event))
        raw       <- consumeOne(container, "media.transcoded")
        decoded   <- ZIO.fromEither(raw.fromJson[TranscodedEvent])
      yield assertTrue(decoded.trackId == trackId) &&
            assertTrue(decoded.fileSizeBytes == 55555L)
    },

    test("sendToDlq sends the exact key and value to the DLQ topic") {
      for
        container <- ZIO.service[KafkaContainer]
        _         <- withProducer(container)(_.sendToDlq("my-key", "raw-payload"))
        raw       <- consumeOne(container, "media.uploaded.dlq")
      yield assertTrue(raw == "raw-payload")
    }

  ).provideShared(
    KafkaContainerLayer.live
  ) @@ withLiveClock @@ timeout(2.minutes) @@ sequential
