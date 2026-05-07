package io.streamvault.pipeline.domain

import zio.json.*
import java.util.UUID

final case class TrackUploadedEvent(
  trackId:              UUID,
  filePath:             String,
  mimeType:             String,
  originalFilename:     String,
  downloadUrl:          String,
  uploadUrl:            String,
  transcodedStoredPath: String
) derives JsonDecoder, JsonEncoder

final case class MetadataReadyEvent(
  trackId:     UUID,
  title:       Option[String],
  artist:      Option[String],
  album:       Option[String],
  year:        Option[Int],
  trackNumber: Option[Int],
  discNumber:  Option[Int],
  durationMs:  Option[Int],
  genre:       Option[String]
) derives JsonEncoder, JsonDecoder

final case class TranscodedEvent(
  trackId:        UUID,
  transcodedPath: String,
  mimeType:       String,
  fileSizeBytes:  Long
) derives JsonEncoder, JsonDecoder
