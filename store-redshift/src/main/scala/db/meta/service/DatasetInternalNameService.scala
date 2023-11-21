package db.meta.service

import db.meta.repository.DatasetInternalNameRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class DatasetInternalNameService
(
  val datasetInternalNameRepository: DatasetInternalNameRepository
) {

}