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
    ConsumerSettings[Task, String, String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)
      .withGroupId(cfg.kafka.consumerGroup)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)

  private val producerSettings =
    ProducerSettings[Task, String, String]
      .withBootstrapServers(cfg.kafka.bootstrapServers)

  def consume: Task[Unit] =
    KafkaProducer
      .stream(producerSettings)
      .flatMap { rawProducer =>
        val ep = EventProducer(rawProducer, cfg.kafka.topics)
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
      record: ConsumerRecord[String, String],
      ep: EventProducer
  ): Task[Unit] =
    record.value.fromJson[TrackUploadedEvent] match
      case Left(err) =>
        ZIO.logWarning(s"Deserialization failed key=${record.key}: $err") *>
          ep.sendToDlq(record.key, record.value)
      case Right(event) =>
        ZIO.logInfo(s"Received media.uploaded trackId=${event.trackId}") *>
          processEvent(event, ep).catchAll { e =>
            ZIO.logError(s"Processing failed trackId=${event.trackId}: $e") *>
              ep.sendToDlq(record.key, record.value)
          }

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
