package com.socrata.db.meta.repository

import com.socrata.db.meta.entity._
import io.quarkus.hibernate.orm.panache._
import jakarta.enterprise.context.ApplicationScoped

import scala.compat.java8.OptionConverters._

@ApplicationScoped
class DatasetColumnRepository extends PanacheRepository[DatasetColumn] {
  def findByDatasetIdAndColumnId(datasetId: Long, columnId: Long): Option[DatasetColumn] = {
    find(
      "datasetId = ?1 and columnId = ?2",
      Seq(datasetId, java.lang.Long.valueOf(columnId)).map(_.asInstanceOf[Object]): _*
    ).singleResultOptional().asScala
  }
}
