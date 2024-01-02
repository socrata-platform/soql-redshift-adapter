package com.socrata.db.meta.service

import com.socrata.db.meta.repository._
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.entity._
import com.socrata.db.Exists

@ApplicationScoped
class DatasetColumnService(
    private val datasetColumnRepository: DatasetColumnRepository
) {
  def persist(datasetColumn: DatasetColumn): Exists.Exists[DatasetColumn] = {
    // not quite right
    datasetColumnRepository.persist(datasetColumn)
    Exists.DoesNot(datasetColumn)
  }
}
