package db.meta.service

import db.meta.repository.{DatasetInternalNameRepository, DatasetRepository}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class DatasetInternalNameService
(
  val datasetInternalNameRepository: DatasetInternalNameRepository
) {

}
