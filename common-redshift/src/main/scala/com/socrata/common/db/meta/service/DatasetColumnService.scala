package com.socrata.common.db.meta.service


import com.socrata.common.db.Exists
import com.socrata.common.db.meta.entity.DatasetColumn
import com.socrata.common.db.meta.repository.DatasetColumnRepository
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
