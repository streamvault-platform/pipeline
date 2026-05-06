# Streamvault Pipeline — Claude Instructions

## Project
Async media processing service for Streamvault.
Consumes Kafka events, runs transcoding, metadata enrichment, watch sync packaging.
Scala 3 · ZIO 2 · fs2-kafka · FFmpeg (subprocess) · SBT

## Stack constraints
- Effect system: ZIO 2 only. Never Future, cats-effect, or Akka/Pekko.
- Kafka consumption: fs2-kafka with ZIO interop (zio-interop-cats). Never Kafka Streams DSL.
- FFmpeg: invoked via zio-process as subprocess. Never Runtime.exec or ProcessBuilder.
- HTTP (client + server): zio-http. Used for pre-signed URL download/upload and the health/metrics endpoints.
- Config: zio-config-typesafe (application.conf). Never hardcode values.
- Logging: zio-logging → structured JSON output.
- Build: SBT with sbt-assembly for fat JAR. Scala 3.x only.

## Code style
- Full ZIO environment via ZLayer for all dependencies. No global mutable state.
- Domain errors as sealed traits/enums, surfaced via ZIO.fail. Never throw.
- ZIO Streams for all Kafka consumption pipelines — handle backpressure explicitly.
- Tests: zio-test only. Testcontainers for Kafka integration tests.
- Every ZIO fiber doing blocking work must be shifted to blocking thread pool.

## File structure
src/main/scala/io/streamvault/pipeline/
  consumer/     ← Kafka consumer stream definitions
  jobs/         ← Individual processing jobs (transcode, metadata, watchsync)
  domain/       ← Data types, error types, job models
  infra/        ← FFmpeg subprocess, external API clients
  config/       ← ZLayer config definitions

## Kafka topics consumed
- media.uploaded         → run transcode + metadata enrichment
- watch.sync-requested   → build watch-optimized audio package

## Kafka topics produced
- media.transcoded
- media.metadata-ready
- watch.sync-ready

## External integrations
- MusicBrainz API (metadata lookup by AcoustID fingerprint)
- AcoustID API (audio fingerprinting via fpcalc subprocess)
- FFmpeg (transcoding to AAC/OPUS for watch, HLS for video)

## Do NOT
- Use Future anywhere in the codebase
- Block ZIO fibers with Thread.sleep (use ZIO.sleep)
- Use Akka, Pekko, or cats-effect directly
- Hardcode Kafka topic names (all via config)
- Add Akka Streams or Kafka Streams DSL