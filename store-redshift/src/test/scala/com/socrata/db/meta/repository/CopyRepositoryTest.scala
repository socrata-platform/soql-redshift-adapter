package com.socrata.db.meta.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.socrata.db.meta.entity.{CopyInfo, DatasetInfo}
import io.quarkus.logging.Log
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import jakarta.transaction.Transactional
import org.junit.jupiter.api.Test

import java.time.LocalDateTime

@QuarkusTest
class CopyRepositoryTest {

  @Inject
  var copyRepository: CopyRepository = _
  @Inject
  var datasetRepository: DatasetRepository = _

  @Inject
  var objectMapper: ObjectMapper = _

  @Transactional
  @Test
  def findByDatasetInternalNameAndCopyNumber(): Unit = {
    val di: DatasetInfo = DatasetInfo(0L, "en", Array.empty, Some("aaaa-aaaa"), 1L)
    datasetRepository.persist(di)
    val ci: CopyInfo = CopyInfo(di,1L,"published",1L,LocalDateTime.now,Some(1L))
    copyRepository.persist(ci)
    val res: Option[CopyInfo] = copyRepository.findByDatasetResourceNameAndCopyNumber("aaaa-aaaa", 1L)
    Log.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res))
  }
}
