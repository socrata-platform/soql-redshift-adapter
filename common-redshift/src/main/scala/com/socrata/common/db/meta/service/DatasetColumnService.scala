package com.socrata.common.db.meta.service

import com.socrata.common.db.meta.entity.DatasetColumn
import com.socrata.common.db.meta.repository.DatasetColumnRepository
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DatasetColumnService(
    private val datasetColumnRepository: DatasetColumnRepository
) {

  val findByDatasetIdAndUserColumnId =
    (datasetColumnRepository.findByDatasetIdAndUserColumnId _)

  def persist(datasetColumn: DatasetColumn): DatasetColumn = {
    datasetColumnRepository.findByDatasetIdAndColumnId(
      datasetColumn.datasetId,
      datasetColumn.columnId
    ) match {
      case Some(found) =>
        DatasetColumn.update(found, datasetColumn)
        datasetColumnRepository.persist(found)
        found
      case None =>
        datasetColumnRepository.persist(datasetColumn)
        datasetColumn
    }
  }
}
