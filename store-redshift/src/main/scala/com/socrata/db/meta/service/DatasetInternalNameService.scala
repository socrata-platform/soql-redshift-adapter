package com.socrata.db.meta.service

import com.socrata.db.meta.repository.DatasetInternalNameRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class DatasetInternalNameService
(
  val datasetInternalNameRepository: DatasetInternalNameRepository
) {

}
