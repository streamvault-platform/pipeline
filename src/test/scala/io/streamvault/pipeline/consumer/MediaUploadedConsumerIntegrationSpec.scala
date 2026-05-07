package io.streamvault.pipeline.consumer

import fs2.kafka.*
import io.streamvault.pipeline.KafkaContainerLayer
import io.streamvault.pipeline.domain.TrackUploadedEvent
import io.streamvault.pipeline.infra.FileDownloader
import org.testcontainers.containers.KafkaContainer
import zio.*
import zio.interop.catz.*
import zio.json.*
import zio.test.*
import zio.test.TestAspect.*

import java.util.UUID

object MediaUploadedConsumerIntegrationSpec extends ZIOSpecDefault:

  private def produce(container: KafkaContainer, topic: String, key: String, value: String): Task[Unit] =
    val bs = KafkaContainerLayer.bootstrapServers(container)
    KafkaProducer
      .stream(ProducerSettings[Task, String, String].withBootstrapServers(bs))
      .evalMap(p => p.produce(ProducerRecords.one(ProducerRecord(topic, key, value))).flatten.unit)
      .take(1)
      .compile
      .drain

  // Reads the first DLQ message satisfying `pred`. Using Earliest so we catch messages
  // produced before the consumer subscribes, but filtering lets each test find its own.
  private def consumeMatching(container: KafkaContainer, topic: String, pred: String => Boolean): Task[String] =
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
      .map(_.record.value)
      .filter(pred)
      .take(1)
      .compile
      .lastOrError

  // Runs the full consumer loop in a background fiber; interrupts it after the body completes.
  private def withConsumer[A](container: KafkaContainer, groupId: String)(body: Task[A]): Task[A] =
    val cfg = KafkaContainerLayer.appConfig(container, consumerGroup = groupId)
    MediaUploadedConsumer.consume
      .provide(ZLayer.succeed(cfg), FileDownloader.live, MediaUploadedConsumer.live)
      .fork
      .flatMap(fiber => body.ensuring(fiber.interrupt.ignore))

  def spec = suite("MediaUploadedConsumer — integration")(

    test("malformed JSON is routed to the DLQ without crashing the consumer") {
      for
        container <- ZIO.service[KafkaContainer]
        groupId    = s"test-${UUID.randomUUID()}"
        _         <- produce(container, "media.uploaded", "k1", "{not valid json")
        dlqValue  <- withConsumer(container, groupId) {
                       consumeMatching(container, "media.uploaded.dlq", _ == "{not valid json")
                         .timeoutFail(new Exception("DLQ timeout after 30s"))(30.seconds)
                     }
      yield assertTrue(dlqValue == "{not valid json")
    },

    test("valid event with an unreachable download URL is routed to the DLQ") {
      val event = TrackUploadedEvent(
        trackId              = UUID.randomUUID(),
        filePath             = "irrelevant",
        mimeType             = "audio/mpeg",
        originalFilename     = "song.mp3",
        downloadUrl          = "http://127.0.0.1:9999/song.mp3", // connection refused immediately
        uploadUrl            = "http://127.0.0.1:9999/upload",
        transcodedStoredPath = "tracks/transcoded/song.aac"
      )
      for
        container <- ZIO.service[KafkaContainer]
        groupId    = s"test-${UUID.randomUUID()}"
        _         <- produce(container, "media.uploaded", event.trackId.toString, event.toJson)
        dlqValue  <- withConsumer(container, groupId) {
                       consumeMatching(container, "media.uploaded.dlq", _.contains(event.trackId.toString))
                         .timeoutFail(new Exception("DLQ timeout after 30s"))(30.seconds)
                     }
      yield assertTrue(dlqValue.contains(event.trackId.toString))
    }

  ).provideShared(
    KafkaContainerLayer.live
  ) @@ withLiveClock @@ timeout(3.minutes) @@ sequential
