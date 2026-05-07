package io.streamvault.pipeline.jobs

import io.streamvault.pipeline.TestEventSink
import io.streamvault.pipeline.domain.TrackUploadedEvent
import zio.*
import zio.test.*

import java.nio.file.{Files, Paths}
import java.util.UUID

// Requires fixture files under src/test/resources/fixtures/
// Run src/test/scripts/generate-test-fixtures.sh to create them.
object MetadataExtractorSpec extends ZIOSpecDefault:

  private def fixturePath(name: String) =
    Paths.get(getClass.getResource(s"/fixtures/$name").toURI)

  private def fakeEvent(originalFilename: String) = TrackUploadedEvent(
    trackId              = UUID.randomUUID(),
    filePath             = "irrelevant",
    mimeType             = "audio/mpeg",
    originalFilename     = originalFilename,
    downloadUrl          = "http://irrelevant",
    uploadUrl            = "http://irrelevant",
    transcodedStoredPath = "irrelevant"
  )

  def spec = suite("MetadataExtractor")(

    suite("extract — tagged file")(
      test("reads title, artist, album from ID3 tags") {
        val event = fakeEvent("tagged.mp3")
        for
          sink <- TestEventSink.make
          _    <- MetadataExtractor.extract(event, fixturePath("tagged.mp3"), sink)
          evts <- sink.metadataEvents
        yield assertTrue(evts.length == 1) &&
              assertTrue(evts.head.title  == Some("Test Title")) &&
              assertTrue(evts.head.artist == Some("Test Artist")) &&
              assertTrue(evts.head.album  == Some("Test Album"))
      },
      test("durationMs is populated and positive") {
        val event = fakeEvent("tagged.mp3")
        for
          sink <- TestEventSink.make
          _    <- MetadataExtractor.extract(event, fixturePath("tagged.mp3"), sink)
          evts <- sink.metadataEvents
        yield assertTrue(evts.head.durationMs.exists(_ > 0))
      },
      test("trackId in the produced event matches the input event") {
        val event = fakeEvent("tagged.mp3")
        for
          sink <- TestEventSink.make
          _    <- MetadataExtractor.extract(event, fixturePath("tagged.mp3"), sink)
          evts <- sink.metadataEvents
        yield assertTrue(evts.head.trackId == event.trackId)
      }
    ),

    suite("extract — untagged file")(
      test("falls back to filename (sans extension) for title") {
        val event = fakeEvent("untagged.mp3")
        for
          sink <- TestEventSink.make
          _    <- MetadataExtractor.extract(event, fixturePath("untagged.mp3"), sink)
          evts <- sink.metadataEvents
        yield assertTrue(evts.head.title == Some("untagged"))
      },
      test("artist and album are None when tag block is absent") {
        val event = fakeEvent("untagged.mp3")
        for
          sink <- TestEventSink.make
          _    <- MetadataExtractor.extract(event, fixturePath("untagged.mp3"), sink)
          evts <- sink.metadataEvents
        yield assertTrue(evts.head.artist.isEmpty) &&
              assertTrue(evts.head.album.isEmpty)
      }
    ),

    suite("extract — failure handling")(
      test("fails with Throwable when the audio file does not exist") {
        val event    = fakeEvent("missing.mp3")
        val badPath  = Paths.get("/tmp/does-not-exist-ever.mp3")
        for
          sink   <- TestEventSink.make
          result <- MetadataExtractor.extract(event, badPath, sink).exit
          dlqs   <- sink.dlqEvents
        yield assertTrue(result.isFailure) &&
              assertTrue(dlqs.isEmpty) // extract does not DLQ — caller does
      }
    )
  )
