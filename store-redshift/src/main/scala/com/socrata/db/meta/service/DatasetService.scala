package com.socrata.db.meta.service

import com.socrata.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.entity._

@ApplicationScoped
class DatasetService(
    private val datasetRepository: DatasetRepository
) {
  def insert(dataset: Dataset) = datasetRepository.persist(dataset)
}
