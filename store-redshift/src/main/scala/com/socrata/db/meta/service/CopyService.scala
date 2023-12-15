package com.socrata.db.meta.service

import com.socrata.db.meta.entity.CopyInfo
import com.socrata.db.meta.repository.{CopyOps, CopyRepository}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

import java.util.Optional

@Transactional
@ApplicationScoped
class CopyService
(
  private val copyRepository: CopyRepository
) extends CopyOps {

  override def findByDatasetResourceNameAndCopyNumber(resourceName: String, copyNumber: Long): Option[CopyInfo] = {
    copyRepository.findByDatasetResourceNameAndCopyNumber(resourceName, copyNumber)
  }
}
