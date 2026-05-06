package io.streamvault.pipeline.domain

import java.util.UUID

sealed trait PipelineError

object PipelineError:
  final case class DeserializationError(raw: String, message: String)  extends PipelineError
  final case class DownloadError(url: String, cause: Throwable)        extends PipelineError
  final case class UploadError(url: String, cause: Throwable)          extends PipelineError
  final case class MetadataExtractionError(trackId: UUID, cause: Throwable) extends PipelineError
  final case class TranscodeError(trackId: UUID, cause: Throwable)     extends PipelineError
  final case class KafkaProducerError(topic: String, cause: Throwable) extends PipelineError
