# streamvault-pipeline

[![CI](https://github.com/streamvault-app/streamvault-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/streamvault-app/streamvault-pipeline/actions/workflows/ci.yml)
![Scala](https://img.shields.io/badge/scala-3.x-red)
![ZIO](https://img.shields.io/badge/zio-2.x-green)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)

Async media processing for [Streamvault](https://github.com/streamvault-app) — handles everything CPU/IO-heavy that the core API shouldn't block on.

**Scala 3 · ZIO 2 · fs2-kafka · FFmpeg**

Consumes `media.uploaded`, runs metadata extraction and transcoding in parallel, produces results back to Kafka. No database, no storage credentials — file access goes entirely through pre-signed URLs received in the Kafka event.

---

## What it does

On each `media.uploaded` event, two things run in parallel:

1. **Metadata extraction** — HTTP GET the file, extract ID3 tags via JAudioTagger (falls back to filename), produce `media.metadata-ready`
2. **Transcode** — HTTP GET the file, FFmpeg → AAC 128kbps, HTTP PUT result to the upload URL, produce `media.transcoded`

Failed events go to a `.dlq` topic; the consumer offset is committed regardless so one bad track doesn't stall everything.

---

## Running locally

```sh
# Full stack (Postgres, Kafka, RustFS, Redis, Envoy, core)
cd ../infra && docker compose up -d

# Dev run
sbt run
```

FFmpeg must be on `PATH`.

---

## Tests

```sh
sbt unit-test         # fast, no Docker required (ffmpeg must be on PATH)
sbt integration-test  # Kafka via Testcontainers, needs Docker
```

---

## Configuration

| Variable | Description |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | e.g. `streamvault-kafka:9092` |
| `KAFKA_TOPICS_MEDIA_UPLOADED` | Input topic |
| `KAFKA_TOPICS_METADATA_READY` | Output topic |
| `KAFKA_TOPICS_TRANSCODED` | Output topic |

No S3 config — file access uses pre-signed URLs from the `media.uploaded` event.

Full variable reference in `../infra/.env.example`.

---

## Architecture

```
consumer/   ← fs2-kafka stream, at-least-once delivery
jobs/       ← MetadataExtractor, Transcoder
domain/     ← event types, error types
infra/      ← EventProducer, FileDownloader, HealthServer
config/     ← AppConfig
```

Health: `GET /health` · Metrics: `GET /metrics` (Prometheus)
