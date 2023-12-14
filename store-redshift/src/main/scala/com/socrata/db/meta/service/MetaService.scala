package com.socrata.db.meta.service

import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class MetaService
(
  private val datasetService: DatasetService,
  private val copyService: CopyService,
  private val columnService: ColumnService
) {

}
