package meta.service

import jakarta.enterprise.context.ApplicationScoped
import meta.repository.{DatasetInternalNameRepository, DatasetRepository}

@ApplicationScoped
class DatasetInternalNameService
(
  datasetInternalNameRepository: DatasetInternalNameRepository
) {

}
