package com.socrata.db.meta.service

import com.socrata.db.Exists
import com.socrata.db.meta.entity._
import com.socrata.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped

@jakarta.transaction.Transactional
@ApplicationScoped
class DatasetService(
    private val datasetRepository: DatasetRepository
) {

  val findByInternalNameAndPublishedState = (datasetRepository.findByInternalNameAndPublishedState _)

  def persist(dataset: Dataset): Exists.Exists[Dataset] = {
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
