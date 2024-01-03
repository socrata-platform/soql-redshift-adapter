package com.socrata.db.meta.service

import com.socrata.db.Exists
import com.socrata.db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.entity._
import scala.compat.java8.OptionConverters._

@jakarta.transaction.Transactional
@ApplicationScoped
class DatasetService(
    private val datasetRepository: DatasetRepository
) {
  def persist(dataset: Dataset): Exists.Exists[Dataset] = {
    datasetRepository.findByInternalNameAndCopyNumber(dataset.internalName, dataset.copyNumber) match {
      case Some(found) =>
        dataset.systemId = found.systemId
        datasetRepository.persist(dataset)
        Exists.Updated(dataset)
      case None =>
        datasetRepository.persist(dataset)
        Exists.Inserted(dataset)
    }
  }
}
