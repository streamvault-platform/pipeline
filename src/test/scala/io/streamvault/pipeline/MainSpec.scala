package io.streamvault.pipeline

import zio.test.*

object MainSpec extends ZIOSpecDefault:
  def spec = suite("Pipeline")(
    test("placeholder") {
      assertTrue(true)
    }
  )
