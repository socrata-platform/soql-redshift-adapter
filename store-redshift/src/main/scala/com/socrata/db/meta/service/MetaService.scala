package com.socrata.db.meta.service

import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class MetaService
(
  val datasetService: DatasetService,
  val datasetInternalNameService: DatasetInternalNameService
) {

}
