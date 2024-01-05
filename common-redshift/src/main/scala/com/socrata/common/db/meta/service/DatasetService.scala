package com.socrata.common.db.meta.service

import com.socrata.common.db.Exists
import com.socrata.common.db.Exists.Exists
import com.socrata.common.db.meta.entity.Dataset
import com.socrata.common.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped

@jakarta.transaction.Transactional
@ApplicationScoped
class DatasetService(
    private val datasetRepository: DatasetRepository
) {
  def persist(dataset: Dataset): Exists[Dataset] = {
    datasetRepository.findByInternalNameAndCopyNumber(dataset.internalName, dataset.copyNumber) match {
      case Some(found) =>
        Dataset.update(found, dataset)
        datasetRepository.persist(found)
        Exists.Updated(found)
      case None =>
        datasetRepository.persist(dataset)
        Exists.Inserted(dataset)
    }
  }
}
