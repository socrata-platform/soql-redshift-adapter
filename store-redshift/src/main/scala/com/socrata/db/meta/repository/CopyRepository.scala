package com.socrata.db.meta.repository

import com.socrata.db.meta.entity.CopyInfo
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped
import scala.compat.java8.OptionConverters._
@ApplicationScoped
class CopyRepository extends PanacheRepository[CopyInfo] with CopyOps {

  override def findByDatasetResourceNameAndCopyNumber(resourceName: String, copyNumber: Long): Option[CopyInfo] = {
    find("datasetInfo.resourceName = ?1 and copyNumber = ?2",Seq(Some(resourceName),java.lang.Long.valueOf(copyNumber)): _*).singleResultOptional().asScala
  }

}

trait CopyOps {
  def findByDatasetResourceNameAndCopyNumber(resourceName: String, copyNumber: Long): Option[CopyInfo]
}
