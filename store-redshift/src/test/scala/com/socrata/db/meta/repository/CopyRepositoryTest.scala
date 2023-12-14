package com.socrata.db.meta.repository

import io.quarkus.logging.Log
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@QuarkusTest
class CopyRepositoryTest {

  @Inject
  var copyRepository: CopyRepository = _

  @Test
  def findByDatasetInternalNameAndCopyNumber(): Unit = {
    val res = copyRepository.findByDatasetResourceNameAndCopyNumber("one",1)
    Log.info(res)
  }
}
