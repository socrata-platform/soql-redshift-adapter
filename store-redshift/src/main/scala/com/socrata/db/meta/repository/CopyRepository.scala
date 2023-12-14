package com.socrata.db.meta.repository

import com.socrata.db.meta.entity.CopyInfo
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped
import java.util.Optional

@ApplicationScoped
class CopyRepository extends PanacheRepository[CopyInfo] with CopyOps {

  override def findByDatasetInternalNameAndCopyNumber(datasetInternalName: String, copyNumber: Long): Optional[CopyInfo] = {
    find("copyNumber = ?1", copyNumber.asInstanceOf[Object]).singleResultOptional()
  }

}

trait CopyOps {
  def findByDatasetInternalNameAndCopyNumber(datasetInternalName: String, copyNumber: Long): Optional[CopyInfo]
}
