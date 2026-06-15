package io.streamvault.pipeline.consumer

import fs2.kafka.*
import io.streamvault.pipeline.config.AppConfig
import io.streamvault.pipeline.domain.{WatchSyncReadyEvent, WatchSyncRequestedEvent}
import io.streamvault.pipeline.infra.EventProducer
import zio.*
import zio.interop.catz.*
import zio.json.*

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration as JDuration
import scala.concurrent.duration.*

trait WatchSyncConsumer:
  def consume: Task[Unit]

object WatchSyncConsumer:
  val live: ZLayer[AppConfig, Nothing, WatchSyncConsumer] =
    ZLayer.fromFunction(LiveWatchSyncConsumer(_))

  def consume: ZIO[WatchSyncConsumer, Throwable, Unit] =
    ZIO.serviceWithZIO[WatchSyncConsumer](_.consume)

private final class LiveWatchSyncConsumer(cfg: AppConfig) extends WatchSyncConsumer:

  private val httpClient = HttpClient.newBuilder()
    .connectTimeout(JDuration.ofSeconds(10))
    .build()

  private val consumerSettings =
    ConsumerSettings[Task, Option[String], String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)
      .withGroupId(cfg.kafka.consumerGroup)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)

  private val producerSettings =
    ProducerSettings[Task, String, String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)

  def consume: Task[Unit] =
    ZIO.logInfo(
      s"action=kafka_consumer_start topic=${cfg.kafka.topics.watchSyncRequested} bootstrap=${cfg.kafka.bootstrapServers} group=${cfg.kafka.consumerGroup}"
    ) *>
      KafkaProducer
        .stream(producerSettings)
        .flatMap { rawProducer =>
          val ep = EventProducer(rawProducer, cfg.kafka.topics, cfg.kafka.bootstrapServers)
          KafkaConsumer
            .stream(consumerSettings)
            .evalTap(_.subscribeTo(cfg.kafka.topics.watchSyncRequested))
            .flatMap { consumer =>
              consumer.stream
                .mapAsync(4) { committable =>
                  handleRecord(committable.record, ep).as(committable.offset)
                }
            }
        }
        .through(commitBatchWithin(500, 15.seconds))
        .compile
        .drain

  private def handleRecord(
      record: ConsumerRecord[Option[String], String],
      ep: EventProducer
  ): Task[Unit] =
    val keyStr = record.key.getOrElse("<null>")
    ZIO.logDebug(
      s"action=kafka_consume topic=${cfg.kafka.topics.watchSyncRequested} key=$keyStr"
    ) *>
      (record.value.fromJson[WatchSyncRequestedEvent] match
        case Left(err) =>
          ZIO.logWarning(
            s"action=kafka_deserialize_failed topic=${cfg.kafka.topics.watchSyncRequested} key=$keyStr error=$err"
          ) *>
            ep.sendToWatchSyncDlq(record.key.orNull, record.value).ignore

        case Right(event) =>
          ZIO.logInfo(
            s"action=kafka_consume_ok topic=${cfg.kafka.topics.watchSyncRequested} syncRequestId=${event.syncRequestId} deviceId=${event.deviceId} trackCount=${event.tracks.size}"
          ) *>
            processEvent(event, ep)
              .timeoutFail(new Exception("watch sync processing timeout after 5 min"))(5.minutes)
              .catchAll { e =>
                ZIO.logError(
                  s"action=watch_sync_process_failed syncRequestId=${event.syncRequestId} error=$e"
                ) *>
                  ep.sendToWatchSyncDlq(record.key.orNull, record.value).ignore
              })

  private def processEvent(event: WatchSyncRequestedEvent, ep: EventProducer): Task[Unit] =
    for
      manifest <- ZIO.foreachPar(event.tracks) { track =>
                    fileSizeBytes(track.downloadUrl).map { size =>
                      WatchSyncReadyEvent.ManifestEntry(track.trackId, track.downloadUrl, size)
                    }
                  }
      ready     = WatchSyncReadyEvent(event.syncRequestId, event.userId, event.deviceId, manifest)
      _        <- ep.produceWatchSyncReady(ready)
      _        <- ZIO.logInfo(
                    s"action=watch_sync_ready_produced syncRequestId=${event.syncRequestId} deviceId=${event.deviceId} trackCount=${manifest.size}"
                  )
    yield ()

  // HEAD request to get Content-Length; falls back to 0 if unsupported or unreachable.
  private def fileSizeBytes(url: String): Task[Long] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(url))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .timeout(JDuration.ofSeconds(10))
        .build()
      val resp = httpClient.send(req, HttpResponse.BodyHandlers.discarding())
      resp.headers().firstValueAsLong("content-length").orElse(0L)
    }.orElse(ZIO.succeed(0L))
