package com.socrata.db.meta.service

import com.socrata.db.Exists
import com.socrata.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.entity._

@ApplicationScoped
class DatasetService(
    private val datasetRepository: DatasetRepository
) {
  def persist(dataset: Dataset): Exists.Exists[Dataset] = ??? // Does(datasetRepository.persist(dataset))
}
