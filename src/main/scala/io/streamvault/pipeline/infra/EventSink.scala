package io.streamvault.pipeline.infra

import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent, WatchSyncReadyEvent}
import zio.*

trait EventSink:
  def produceMetadataReady(event: MetadataReadyEvent): Task[Unit]
  def produceTranscoded(event: TranscodedEvent): Task[Unit]
  def produceWatchSyncReady(event: WatchSyncReadyEvent): Task[Unit]
  def sendToDlq(key: String, value: String): Task[Unit]
  def sendToWatchSyncDlq(key: String, value: String): Task[Unit]
