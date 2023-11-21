package db.meta.service

import db.meta.repository.DatasetRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class DatasetService
(
  val datasetRepository: DatasetRepository
) {

}