package com.socrata.db.meta.service

import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@Transactional
@ApplicationScoped
class ColumnService
(
  private val columnService: ColumnService
) {

}
