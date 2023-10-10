package db.meta.service

import db.meta.repository.{DatasetInternalNameRepository, DatasetRepository}
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class MetaService
(
  val datasetService: DatasetService,
  val datasetInternalNameService: DatasetInternalNameService
) {

}
