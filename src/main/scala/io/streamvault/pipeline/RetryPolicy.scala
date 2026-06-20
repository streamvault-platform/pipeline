package io.streamvault.pipeline

import zio.*
import scala.concurrent.duration.*

object RetryPolicy:
  /** 3 retries with exponential backoff: 1s → 2s → 4s (up to 7s total wait). */
  val transient = Schedule.exponential(1.second) && Schedule.recurs(3)
