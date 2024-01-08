package com.socrata.common.db.meta.repository

import com.socrata.common.db.meta.entity.Dataset
import io.quarkus.hibernate.orm.panache._
import jakarta.enterprise.context.ApplicationScoped

import scala.compat.java8.OptionConverters._

@ApplicationScoped
class DatasetRepository extends PanacheRepository[Dataset] {

  def findByInternalNameAndPublishedState(
      internalName: String,
      published: Boolean
  ): Option[Dataset] =
    find(
      "internalName = ?1 and published = ?2", // need MAX based on copyNumber
      Seq(internalName, published).map(_.asInstanceOf[Object]): _*
    ).singleResultOptional().asScala

  def findByInternalNameAndCopyNumber(
      internalName: String,
      copyNumber: Long
  ): Option[Dataset] = {
    find(
      "internalName = ?1 and copyNumber = ?2",
      Seq(internalName, copyNumber).map(_.asInstanceOf[Object]): _*
    ).singleResultOptional().asScala
  }

}
