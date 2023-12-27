package com.socrata.store.json
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@QuarkusTest class JsonTransformerTest() {
  @Inject var jsonTransformer: JsonTransformer = _

  @Test def test: Unit = {???}

}
