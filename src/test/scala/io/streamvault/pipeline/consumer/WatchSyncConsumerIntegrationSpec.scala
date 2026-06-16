package io.streamvault.pipeline.consumer

import fs2.kafka.*
import io.streamvault.pipeline.KafkaContainerLayer
import io.streamvault.pipeline.domain.{WatchSyncReadyEvent, WatchSyncRequestedEvent}
import org.testcontainers.containers.KafkaContainer
import zio.*
import zio.interop.catz.*
import zio.json.*
import zio.test.*
import zio.test.TestAspect.*

import java.util.UUID

object WatchSyncConsumerIntegrationSpec extends ZIOSpecDefault:

  private def produce(container: KafkaContainer, topic: String, key: String, value: String): Task[Unit] =
    val bs = KafkaContainerLayer.bootstrapServers(container)
    KafkaProducer
      .stream(ProducerSettings[Task, String, String].withBootstrapServers(bs))
      .evalMap(p => p.produce(ProducerRecords.one(ProducerRecord(topic, key, value))).flatten.unit)
      .take(1)
      .compile
      .drain

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

  private def withConsumer[A](container: KafkaContainer, groupId: String)(body: Task[A]): Task[A] =
    val cfg = KafkaContainerLayer.appConfig(container, consumerGroup = groupId)
    WatchSyncConsumer.consume
      .provide(ZLayer.succeed(cfg), WatchSyncConsumer.live)
      .fork
      .flatMap(fiber => body.ensuring(fiber.interrupt.ignore))

  def spec = suite("WatchSyncConsumer — integration")(

    test("valid event produces watch.sync-ready with manifest") {
      val syncRequestId = UUID.randomUUID()
      val userId        = UUID.randomUUID()
      val trackId       = UUID.randomUUID()
      // WireMock not needed — HEAD on 127.0.0.1:9999 will fail, falling back to fileSizeBytes=0
      val event = WatchSyncRequestedEvent(
        syncRequestId = syncRequestId,
        userId        = userId,
        deviceId      = "watch-abc",
        tracks        = List(WatchSyncRequestedEvent.TrackInfo(trackId, "http://127.0.0.1:9999/track.aac"))
      )
      for
        container <- ZIO.service[KafkaContainer]
        groupId    = s"test-${UUID.randomUUID()}"
        _         <- produce(container, "watch.sync-requested", syncRequestId.toString, event.toJson)
        raw       <- withConsumer(container, groupId) {
                       consumeMatching(container, "watch.sync-ready", _.contains(syncRequestId.toString))
                         .timeoutFail(new Exception("watch.sync-ready timeout after 30s"))(30.seconds)
                     }
        ready     <- ZIO.fromEither(raw.fromJson[WatchSyncReadyEvent])
      yield
        assertTrue(ready.syncRequestId == syncRequestId) &&
        assertTrue(ready.userId        == userId) &&
        assertTrue(ready.deviceId      == "watch-abc") &&
        assertTrue(ready.manifest.size == 1) &&
        assertTrue(ready.manifest.head.trackId == trackId) &&
        assertTrue(ready.manifest.head.fileSizeBytes == 0L)  // HEAD unreachable → fallback
    },

    test("malformed JSON is routed to the DLQ without crashing the consumer") {
      for
        container <- ZIO.service[KafkaContainer]
        groupId    = s"test-${UUID.randomUUID()}"
        _         <- produce(container, "watch.sync-requested", "bad-key", "{not valid json")
        dlqValue  <- withConsumer(container, groupId) {
                       consumeMatching(container, "watch.sync-requested.dlq", _ == "{not valid json")
                         .timeoutFail(new Exception("DLQ timeout after 30s"))(30.seconds)
                     }
      yield assertTrue(dlqValue == "{not valid json")
    },

    test("consumer remains healthy after a bad message") {
      val syncRequestId = UUID.randomUUID()
      val trackId       = UUID.randomUUID()
      val good = WatchSyncRequestedEvent(
        syncRequestId = syncRequestId,
        userId        = UUID.randomUUID(),
        deviceId      = "watch-xyz",
        tracks        = List(WatchSyncRequestedEvent.TrackInfo(trackId, "http://127.0.0.1:9999/t.aac"))
      )
      for
        container <- ZIO.service[KafkaContainer]
        groupId    = s"test-${UUID.randomUUID()}"
        _         <- produce(container, "watch.sync-requested", "bad", "{invalid}")
        _         <- produce(container, "watch.sync-requested", syncRequestId.toString, good.toJson)
        raw       <- withConsumer(container, groupId) {
                       consumeMatching(container, "watch.sync-ready", _.contains(syncRequestId.toString))
                         .timeoutFail(new Exception("watch.sync-ready timeout after 30s"))(30.seconds)
                     }
        ready     <- ZIO.fromEither(raw.fromJson[WatchSyncReadyEvent])
      yield assertTrue(ready.syncRequestId == syncRequestId)
    }

  ).provideShared(
    KafkaContainerLayer.live
  ) @@ withLiveClock @@ timeout(3.minutes) @@ sequential
