package com.socrata.common.db.meta.repository

import com.socrata.common.db.meta.entity.DatasetColumn
import io.quarkus.hibernate.orm.panache._
import jakarta.enterprise.context.ApplicationScoped

import scala.compat.java8.OptionConverters._

@ApplicationScoped
class DatasetColumnRepository extends PanacheRepository[DatasetColumn] {
  def findByDatasetIdAndUserColumnId(datasetId: Long, userColumnId: String): Option[DatasetColumn] = {
    find(
      "datasetId = ?1 and userColumnId = ?2",
      Seq(datasetId, userColumnId).map(_.asInstanceOf[Object]): _*
    ).singleResultOptional().asScala
  }

  def findByDatasetIdAndColumnId(datasetId: Long, columnId: Long): Option[DatasetColumn] = {
    find(
      "datasetId = ?1 and columnId = ?2",
      Seq(datasetId, columnId).map(_.asInstanceOf[Object]): _*
    ).singleResultOptional().asScala
  }
}
