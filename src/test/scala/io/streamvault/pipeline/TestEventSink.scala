package io.streamvault.pipeline

import io.streamvault.pipeline.domain.{MetadataReadyEvent, TranscodedEvent, WatchSyncReadyEvent}
import io.streamvault.pipeline.infra.EventSink
import zio.*

final class TestEventSink(
  metadataCalls:       Ref[List[MetadataReadyEvent]],
  transcodedCalls:     Ref[List[TranscodedEvent]],
  watchSyncReadyCalls: Ref[List[WatchSyncReadyEvent]],
  dlqCalls:            Ref[List[(String, String)]]
) extends EventSink:
  def produceMetadataReady(e: MetadataReadyEvent): Task[Unit]   = metadataCalls.update(_ :+ e)
  def produceTranscoded(e: TranscodedEvent): Task[Unit]         = transcodedCalls.update(_ :+ e)
  def produceWatchSyncReady(e: WatchSyncReadyEvent): Task[Unit] = watchSyncReadyCalls.update(_ :+ e)
  def sendToDlq(key: String, value: String): Task[Unit]         = dlqCalls.update(_ :+ (key, value))
  def sendToWatchSyncDlq(key: String, value: String): Task[Unit] = dlqCalls.update(_ :+ (key, value))

  def metadataEvents: UIO[List[MetadataReadyEvent]]    = metadataCalls.get
  def transcodedEvents: UIO[List[TranscodedEvent]]     = transcodedCalls.get
  def watchSyncReadyEvents: UIO[List[WatchSyncReadyEvent]] = watchSyncReadyCalls.get
  def dlqEvents: UIO[List[(String, String)]]           = dlqCalls.get

object TestEventSink:
  def make: UIO[TestEventSink] =
    for
      m <- Ref.make(List.empty[MetadataReadyEvent])
      t <- Ref.make(List.empty[TranscodedEvent])
      w <- Ref.make(List.empty[WatchSyncReadyEvent])
      d <- Ref.make(List.empty[(String, String)])
    yield TestEventSink(m, t, w, d)
