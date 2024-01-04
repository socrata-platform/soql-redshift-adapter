package com.socrata.db.meta.service

import com.socrata.db.Exists
import com.socrata.db.meta.entity._
import com.socrata.db.meta.repository._
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DatasetColumnService(
    private val datasetColumnRepository: DatasetColumnRepository
) {
  def persist(datasetColumn: DatasetColumn): Exists.Exists[DatasetColumn] = {
    datasetColumnRepository.findByDatasetIdAndColumnId(datasetColumn.datasetId, datasetColumn.columnId) match {
      case Some(found) =>
        DatasetColumn.update(found, datasetColumn)
        datasetColumnRepository.persist(found)
        Exists.Updated(found)
      case None =>
        datasetColumnRepository.persist(datasetColumn)
        Exists.Inserted(datasetColumn)
    }
  }
}
