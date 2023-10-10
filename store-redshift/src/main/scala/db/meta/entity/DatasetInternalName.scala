package db.meta.entity

import io.quarkus.hibernate.orm.panache.PanacheEntity
import jakarta.persistence.{Entity, GeneratedValue, Id, PersistenceUnit}

import java.time.{LocalDateTime, ZonedDateTime}

@Entity
class DatasetInternalName
(
  val dataset:Dataset,
  val name: String,
  val disabled:ZonedDateTime

) extends PanacheEntity {

}
