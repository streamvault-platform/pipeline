package io.streamvault.pipeline.jobs

import io.streamvault.pipeline.domain.{TrackUploadedEvent, TranscodedEvent}
import io.streamvault.pipeline.infra.EventProducer
import zio.*
import zio.process.*

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}

object Transcoder:

  private val httpClient = HttpClient.newHttpClient()

  def transcode(event: TrackUploadedEvent, audioFile: Path, ep: EventProducer): Task[Unit] =
    ZIO.scoped {
      for
        outFile <- tempOutputFile()
        _       <- runFfmpeg(audioFile, outFile)
                     .tapError(e => ZIO.logError(s"FFmpeg failed trackId=${event.trackId}: $e"))
        _       <- uploadFile(outFile, event.uploadUrl)
                     .tapError(e => ZIO.logError(s"Upload failed trackId=${event.trackId}: $e"))
        size    <- ZIO.attemptBlocking(Files.size(outFile))
        _       <- ep.produceTranscoded(TranscodedEvent(
                     trackId        = event.trackId,
                     transcodedPath = event.transcodedStoredPath,
                     mimeType       = "audio/aac",
                     fileSizeBytes  = size
                   )).tapError(e => ZIO.logError(s"Failed to produce transcoded trackId=${event.trackId}: $e"))
      yield ()
    }

  private def tempOutputFile(): ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempFile("sv-transcoded-", ".m4a"))
    )(path => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)

  private def runFfmpeg(input: Path, output: Path): Task[Unit] =
    Command("ffmpeg",
      "-i", input.toString,
      "-c:a", "aac",
      "-b:a", "128k",
      "-y", output.toString
    ).stderr(ProcessOutput.Inherit)
     .stdout(ProcessOutput.Inherit)
     .run
     .mapError(e => new Exception(s"Failed to start FFmpeg: $e"))
     .flatMap { process =>
       process.exitCode.flatMap { code =>
         ZIO.when(code.code != 0)(
           ZIO.fail(new Exception(s"FFmpeg exited with code ${code.code}"))
         ).unit
       }
     }

  private def uploadFile(file: Path, uploadUrl: String): Task[Unit] =
    ZIO.attemptBlocking {
      val request = HttpRequest.newBuilder(URI.create(uploadUrl))
        .PUT(HttpRequest.BodyPublishers.ofFile(file))
        .header("Content-Type", "audio/aac")
        .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
      if response.statusCode() / 100 != 2 then
        throw new Exception(s"Upload failed: HTTP ${response.statusCode()} for $uploadUrl")
    }
