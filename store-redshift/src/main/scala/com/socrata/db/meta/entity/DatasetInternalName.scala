package com.socrata.db.meta.entity

import io.quarkus.hibernate.orm.panache.PanacheEntity
import jakarta.persistence.Entity

import java.time.ZonedDateTime

@Entity
class DatasetInternalName
(
  val dataset: Dataset,
  val name: String,
  val disabled: ZonedDateTime

) extends PanacheEntity {

}
