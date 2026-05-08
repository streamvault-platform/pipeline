# Streamvault Pipeline — Claude Instructions

## Project
Async media processing service for Streamvault.
Scala 3 · ZIO 2 · fs2-kafka · FFmpeg (subprocess) · SBT

Consumes `media.uploaded`, runs metadata extraction and transcoding in parallel, produces results back to Kafka.
For platform-wide scope and Kafka topic definitions, see the root CLAUDE.md.

## Stack constraints
- Effect system: ZIO 2 only. Never Future, cats-effect, or Akka/Pekko.
- Kafka: fs2-kafka with ZIO interop (`zio-interop-cats`). Never Kafka Streams DSL.
- HTTP server: zio-http — health (`GET /health`) and metrics (`GET /metrics`) endpoints only.
- HTTP client: `java.net.http.HttpClient` (Java stdlib) — for pre-signed URL download/upload. Not zio-http.
- FFmpeg: invoked via `zio-process` as a subprocess. Never `Runtime.exec` or `ProcessBuilder`.
- Config: `zio-config-typesafe` (`application.conf` + ENV overrides). Never hardcode values.
- Logging: `zio-logging` → structured JSON. Never `println`.
- Build: SBT with `sbt-assembly` for fat JAR. Scala 3 only.

## Code style
- Full ZIO environment via `ZLayer` for all dependencies. No global mutable state.
- Domain errors as sealed traits/enums, surfaced via `ZIO.fail`. Never throw.
- fs2 Streams for all Kafka pipelines — handle backpressure explicitly.
- `ZIO.attemptBlocking` for all blocking work (FFmpeg, file I/O, HTTP client calls).
- Tests: `zio-test` only. Testcontainers for Kafka integration tests. Never mock Kafka.

## File structure
```
src/main/scala/io/streamvault/pipeline/
  consumer/     ← Kafka consumer stream definitions
  jobs/         ← processing jobs (MetadataExtractor, Transcoder)
  domain/       ← event case classes, error types
  infra/        ← EventProducer, FileDownloader, HealthServer
  config/       ← AppConfig, ZLayer config definitions
```

## Kafka topics
Consumed: `media.uploaded`, `watch.sync-requested` (deferred — watch app not built yet)
Produced: `media.metadata-ready`, `media.transcoded`, `watch.sync-ready` (deferred)

Every consumed topic has a `.dlq` dead-letter counterpart. On failure: produce to DLQ, commit the offset, continue. Never crash the consumer.

## Event schemas
Schemas are a shared contract with core — field names and types must match exactly. Serialised as JSON via `zio-json`.

**Consumed — `media.uploaded`:**
```scala
case class TrackUploadedEvent(
  trackId:              UUID,
  filePath:             String,  // backend path (pipeline doesn't need this)
  mimeType:             String,
  originalFilename:     String,
  downloadUrl:          String,  // pre-signed URL — HTTP GET the original file from here
  uploadUrl:            String,  // pre-signed URL — HTTP PUT the transcoded file here
  transcodedStoredPath: String   // echo back verbatim in TranscodedEvent
)
```

**Produced — `media.metadata-ready`:**
```scala
case class MetadataReadyEvent(
  trackId: UUID, title: Option[String], artist: Option[String],
  album: Option[String], year: Option[Int], trackNumber: Option[Int],
  discNumber: Option[Int], durationMs: Option[Int], genre: Option[String]
)
```

**Produced — `media.transcoded`:**
```scala
case class TranscodedEvent(
  trackId:       UUID,
  transcodedPath: String,  // echo transcodedStoredPath from upload event unchanged
  mimeType:      String,   // "audio/aac"
  fileSizeBytes: Long
)
```

## Processing flow
On each `media.uploaded`, two things run in parallel:

1. **Metadata** — HTTP GET `downloadUrl` → extract ID3 tags (JAudioTagger, fall back to filename) → produce `media.metadata-ready`
2. **Transcode** — HTTP GET `downloadUrl` → FFmpeg → AAC 128kbps → HTTP PUT `uploadUrl` → produce `media.transcoded` (echo `transcodedStoredPath` verbatim)

`transcodedPath` is for watch sync packages only. Audio streaming always serves the original.

## Storage
Pipeline is storage-agnostic. No S3 credentials, no AWS SDK.
All file access goes through the pre-signed URLs in `media.uploaded`. Works for both RustFS and the filesystem backend — pipeline doesn't care which.

HTTP client timeouts: 10s connect, 30s request (see `FileDownloader`).

## Deferred (do not build yet)
- `watch.sync-requested` consumer and `watch.sync-ready` producer
- AcoustID fingerprinting / MusicBrainz lookup
- Waveform generation / artwork extraction
- Retry with exponential backoff (basic at-least-once + DLQ is enough for MVP)

## Do NOT
- Use `Future` anywhere
- Block ZIO fibers — use `ZIO.attemptBlocking`
- Use Akka, Pekko, or cats-effect directly
- Hardcode Kafka topic names (all via config)
- Talk to RustFS or any storage backend directly — use the pre-signed URLs
- Re-implement anything core owns: DB writes, streaming, auth
