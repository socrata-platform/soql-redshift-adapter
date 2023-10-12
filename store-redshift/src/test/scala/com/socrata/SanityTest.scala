package com.socrata

import io.quarkus.test.junit.QuarkusTest
import org.junit.jupiter.api.Test

@QuarkusTest
class SanityTest() {
  @Test
  def one() = {
    println("Hello world!")
  }

}
