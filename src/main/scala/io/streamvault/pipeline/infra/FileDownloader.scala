package io.streamvault.pipeline.infra

import zio.*
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.nio.file.{Files, Path}

trait FileDownloader:
  def download(url: String, suffix: String): ZIO[Scope, Throwable, Path]

object FileDownloader:
  val live: ZLayer[Any, Nothing, FileDownloader] =
    ZLayer.succeed(LiveFileDownloader())

  def download(url: String, suffix: String): ZIO[FileDownloader & Scope, Throwable, Path] =
    ZIO.serviceWithZIO[FileDownloader](_.download(url, suffix))

private final class LiveFileDownloader() extends FileDownloader:

  private val httpClient = HttpClient.newHttpClient()

  def download(rawUrl: String, suffix: String): ZIO[Scope, Throwable, Path] =
    for
      tempFile <- ZIO.acquireRelease(
                    ZIO.attemptBlocking(Files.createTempFile("sv-", suffix))
                  )(path => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
      request   = HttpRequest.newBuilder(URI.create(rawUrl)).GET().build()
      response <- ZIO.attemptBlocking(
                    httpClient.send(request, HttpResponse.BodyHandlers.ofFile(tempFile))
                  )
      _        <- ZIO.when(response.statusCode() / 100 != 2)(
                    ZIO.fail(new Exception(s"Download failed: HTTP ${response.statusCode()} for $rawUrl"))
                  )
    yield tempFile
