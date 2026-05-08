package io.streamvault.pipeline.consumer

import fs2.kafka.*
import io.streamvault.pipeline.config.AppConfig
import io.streamvault.pipeline.domain.TrackUploadedEvent
import io.streamvault.pipeline.infra.{EventProducer, FileDownloader}
import io.streamvault.pipeline.jobs.{MetadataExtractor, Transcoder}
import zio.*
import zio.interop.catz.*
import zio.json.*

import scala.concurrent.duration.*

trait MediaUploadedConsumer:
  def consume: Task[Unit]

object MediaUploadedConsumer:
  val live: ZLayer[AppConfig & FileDownloader, Nothing, MediaUploadedConsumer] =
    ZLayer.fromFunction(LiveMediaUploadedConsumer(_, _))

  def consume: ZIO[MediaUploadedConsumer, Throwable, Unit] =
    ZIO.serviceWithZIO[MediaUploadedConsumer](_.consume)

private final class LiveMediaUploadedConsumer(cfg: AppConfig, downloader: FileDownloader)
    extends MediaUploadedConsumer:

  private val consumerSettings =
    // Option[String] key — handles null keys (Core sends without a key)
    ConsumerSettings[Task, Option[String], String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)
      .withGroupId(cfg.kafka.consumerGroup)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)

  private val producerSettings =
    ProducerSettings[Task, String, String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)

  def consume: Task[Unit] =
    ZIO.logInfo(s"action=kafka_consumer_start topic=${cfg.kafka.topics.mediaUploaded} bootstrap=${cfg.kafka.bootstrapServers} group=${cfg.kafka.consumerGroup}") *>
      KafkaProducer
        .stream(producerSettings)
        .flatMap { rawProducer =>
          val ep = EventProducer(rawProducer, cfg.kafka.topics, cfg.kafka.bootstrapServers)
          KafkaConsumer
            .stream(consumerSettings)
            .evalTap(_.subscribeTo(cfg.kafka.topics.mediaUploaded))
            .flatMap { consumer =>
              consumer.stream
                .mapAsync(4) { committable =>
                  handleRecord(committable.record, ep)
                    .as(committable.offset)
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
    ZIO.logDebug(s"action=kafka_consume topic=${cfg.kafka.topics.mediaUploaded} bootstrap=${cfg.kafka.bootstrapServers} key=$keyStr payload=${record.value}") *>
      (record.value.fromJson[TrackUploadedEvent] match
        case Left(err) =>
          ZIO.logWarning(s"action=kafka_deserialize_failed topic=${cfg.kafka.topics.mediaUploaded} key=$keyStr error=$err") *>
            ep.sendToDlq(record.key.orNull, record.value)
        case Right(event) =>
          ZIO.logInfo(s"action=kafka_consume_ok topic=${cfg.kafka.topics.mediaUploaded} bootstrap=${cfg.kafka.bootstrapServers} trackId=${event.trackId} filename=${event.originalFilename} mimeType=${event.mimeType} downloadUrl=${event.downloadUrl}") *>
            processEvent(event, ep).catchAll { e =>
              ZIO.logError(s"action=kafka_process_failed topic=${cfg.kafka.topics.mediaUploaded} trackId=${event.trackId} error=$e") *>
                ep.sendToDlq(record.key.orNull, record.value)
            })

  private def processEvent(event: TrackUploadedEvent, ep: EventProducer): Task[Unit] =
    val suffix = fileSuffix(event.originalFilename)
    ZIO.scoped {
      downloader.download(event.downloadUrl, suffix).flatMap { audioFile =>
        MetadataExtractor.extract(event, audioFile, ep)
          .zipPar(Transcoder.transcode(event, audioFile, ep))
          .unit
      }
    }

  private def fileSuffix(filename: String): String =
    val i = filename.lastIndexOf('.')
    if i >= 0 then filename.substring(i) else ""
