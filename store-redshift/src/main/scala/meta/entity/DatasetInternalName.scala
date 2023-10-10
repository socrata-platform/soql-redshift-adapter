package meta.entity

import io.quarkus.hibernate.orm.panache.PanacheEntity
import jakarta.persistence.{Entity, GeneratedValue, Id, PersistenceUnit}

import java.time.{LocalDateTime, ZonedDateTime}

@Entity
class DatasetInternalName
(
  val dataset:Dataset,
  val name: DatasetInternalName,
  val disabled:ZonedDateTime

) extends PanacheEntity {

}
