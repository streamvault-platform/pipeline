package io.streamvault.pipeline.jobs

import io.streamvault.pipeline.domain.{MetadataReadyEvent, TrackUploadedEvent}
import io.streamvault.pipeline.infra.EventSink
import org.jaudiotagger.audio.AudioFileIO
import org.jaudiotagger.tag.FieldKey
import zio.*

import java.nio.file.Path

object MetadataExtractor:

  def extract(event: TrackUploadedEvent, audioFile: Path, ep: EventSink): Task[Unit] =
    for
      metadata <- ZIO.attemptBlocking(readTags(event, audioFile))
                    .tapError(e => ZIO.logError(s"Tag extraction failed trackId=${event.trackId}: $e"))
      _        <- ep.produceMetadataReady(metadata)
                    .tapError(e => ZIO.logError(s"Failed to produce metadata-ready trackId=${event.trackId}: $e"))
    yield ()

  private def readTags(event: TrackUploadedEvent, path: Path): MetadataReadyEvent =
    val f      = AudioFileIO.read(path.toFile)
    val tag    = f.getTag    // null when file has no tag block at all
    val header = f.getAudioHeader

    def field(key: FieldKey): Option[String] =
      Option(tag).flatMap(t => Option(t.getFirst(key))).filter(_.nonEmpty)

    MetadataReadyEvent(
      trackId     = event.trackId,
      title       = field(FieldKey.TITLE).orElse(nameFromFilename(event.originalFilename)),
      artist      = field(FieldKey.ARTIST),
      album       = field(FieldKey.ALBUM),
      year        = field(FieldKey.YEAR).flatMap(_.toIntOption),
      trackNumber = field(FieldKey.TRACK).flatMap(_.toIntOption),
      discNumber  = field(FieldKey.DISC_NO).flatMap(_.toIntOption),
      durationMs  = Some((header.getPreciseTrackLength * 1000).toInt),
      genre       = field(FieldKey.GENRE)
    )

  private def nameFromFilename(filename: String): Option[String] =
    val i = filename.lastIndexOf('.')
    Some(if i >= 0 then filename.substring(0, i) else filename)
