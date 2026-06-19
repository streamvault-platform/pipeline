package io.streamvault.pipeline

import zio.*
import zio.test.*

object RetryPolicySpec extends ZIOSpecDefault:

  def spec = suite("RetryPolicy")(

    test("makes exactly 4 attempts (1 initial + 3 retries) before propagating failure") {
      for
        attempts <- Ref.make(0)
        fiber    <- (attempts.update(_ + 1) *> ZIO.fail(new Exception("boom")))
                      .retry(RetryPolicy.transient)
                      .exit
                      .fork
        // Advance past all retry delays: 1s + 2s + 4s = 7s
        _        <- TestClock.adjust(30.seconds)
        exit     <- fiber.join
        count    <- attempts.get
      yield assertTrue(count == 4) && assertTrue(exit.isFailure)
    },

    test("succeeds when a later retry recovers") {
      for
        attempts <- Ref.make(0)
        fiber    <- (for
                      n <- attempts.updateAndGet(_ + 1)
                      _ <- ZIO.when(n < 3)(ZIO.fail(new Exception("transient")))
                    yield n)
                      .retry(RetryPolicy.transient)
                      .fork
        _        <- TestClock.adjust(30.seconds)
        value    <- fiber.join
      yield assertTrue(value == 3)
    },

    test("does not retry when first attempt succeeds") {
      for
        attempts <- Ref.make(0)
        // ZIO.attempt gives error type Throwable so .retry compiles
        _        <- (attempts.update(_ + 1) *> ZIO.attempt(()))
                      .retry(RetryPolicy.transient)
        count    <- attempts.get
      yield assertTrue(count == 1)
    }

  )
