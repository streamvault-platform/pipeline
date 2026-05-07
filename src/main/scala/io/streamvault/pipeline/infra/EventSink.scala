package io.streamvault.pipeline.infra

import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent}
import zio.*

trait EventSink:
  def produceMetadataReady(event: MetadataReadyEvent): Task[Unit]
  def produceTranscoded(event: TranscodedEvent): Task[Unit]
  def sendToDlq(key: String, value: String): Task[Unit]
