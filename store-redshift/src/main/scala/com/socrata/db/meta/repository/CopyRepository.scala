package com.socrata.db.meta.repository

import com.socrata.db.meta.entity.CopyInfo
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped
import java.util.Optional

@ApplicationScoped
class CopyRepository extends PanacheRepository[CopyInfo] with CopyOps {

  override def findByDatasetResourceNameAndCopyNumber(resourceName: String, copyNumber: Long): Optional[CopyInfo] = {
    find("datasetInfo.resourceName = ?1 and copyNumber = ?2",Seq(Some(resourceName),java.lang.Long.valueOf(copyNumber)): _*).singleResultOptional()
  }

}

trait CopyOps {
  def findByDatasetResourceNameAndCopyNumber(resourceName: String, copyNumber: Long): Optional[CopyInfo]
}
