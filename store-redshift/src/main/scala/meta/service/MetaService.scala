package meta.service

import jakarta.enterprise.context.ApplicationScoped
import meta.repository.{DatasetInternalNameRepository, DatasetRepository}

@ApplicationScoped
class MetaService
(
  val datasetService: DatasetService,
  val datasetInternalNameService: DatasetInternalNameService
) {

}
