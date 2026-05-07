package io.streamvault.pipeline.jobs

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.streamvault.pipeline.TestEventSink
import io.streamvault.pipeline.domain.TrackUploadedEvent
import zio.*
import zio.test.*
import zio.test.TestAspect.*

import java.nio.file.{Files, Paths}
import java.util.UUID

// Requires src/test/resources/fixtures/tagged.mp3 and ffmpeg on PATH.
// Run src/test/scripts/generate-test-fixtures.sh to create the fixture.
object TranscoderSpec extends ZIOSpecDefault:

  private val wireMockLayer: ZLayer[Scope, Nothing, WireMockServer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(
        ZIO.succeed {
          val wm = new WireMockServer(WireMockConfiguration.options().dynamicPort())
          wm.start()
          wm
        }
      )(wm => ZIO.succeed(wm.stop()))
    )

  private def fakeEvent(wm: WireMockServer, originalFilename: String): TrackUploadedEvent =
    TrackUploadedEvent(
      trackId              = UUID.randomUUID(),
      filePath             = "irrelevant",
      mimeType             = "audio/mpeg",
      originalFilename     = originalFilename,
      downloadUrl          = s"http://localhost:${wm.port()}/file",
      uploadUrl            = s"http://localhost:${wm.port()}/upload",
      transcodedStoredPath = "tracks/transcoded/song.aac"
    )

  def spec = suite("Transcoder")(

    test("successful transcode produces a TranscodedEvent with correct fields") {
      val fixturePath = Paths.get(getClass.getResource("/fixtures/tagged.mp3").toURI)
      ZIO.scoped {
        for
          wm   <- ZIO.service[WireMockServer]
          _     = wm.stubFor(put(urlPathEqualTo("/upload")).willReturn(ok()))
          event = fakeEvent(wm, "tagged.mp3")
          sink <- TestEventSink.make
          _    <- Transcoder.transcode(event, fixturePath, sink)
          evts <- sink.transcodedEvents
        yield assertTrue(evts.length == 1) &&
              assertTrue(evts.head.trackId == event.trackId) &&
              assertTrue(evts.head.transcodedPath == "tracks/transcoded/song.aac") &&
              assertTrue(evts.head.mimeType == "audio/aac") &&
              assertTrue(evts.head.fileSizeBytes > 0)
      }
    },

    test("upload returning non-2xx fails the effect and does not produce TranscodedEvent") {
      val fixturePath = Paths.get(getClass.getResource("/fixtures/tagged.mp3").toURI)
      ZIO.scoped {
        for
          wm   <- ZIO.service[WireMockServer]
          _     = wm.stubFor(put(urlPathEqualTo("/upload")).willReturn(serverError()))
          event = fakeEvent(wm, "tagged.mp3")
          sink <- TestEventSink.make
          exit <- Transcoder.transcode(event, fixturePath, sink).exit
          evts <- sink.transcodedEvents
        yield assertTrue(exit.isFailure) &&
              assertTrue(evts.isEmpty)
      }
    },

    test("non-existent input file fails and does not produce TranscodedEvent") {
      ZIO.scoped {
        for
          wm   <- ZIO.service[WireMockServer]
          event = fakeEvent(wm, "missing.mp3")
          sink <- TestEventSink.make
          exit <- Transcoder.transcode(event, Paths.get("/tmp/no-such-file.mp3"), sink).exit
          evts <- sink.transcodedEvents
        yield assertTrue(exit.isFailure) &&
              assertTrue(evts.isEmpty)
      }
    }

  ).provideSomeLayerShared[Scope](wireMockLayer) @@ withLiveClock @@ timeout(2.minutes) @@ sequential
