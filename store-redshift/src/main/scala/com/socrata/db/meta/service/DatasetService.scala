package com.socrata.db.meta.service

import com.socrata.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class DatasetService
(
  private val datasetRepository: DatasetRepository
) {

}
