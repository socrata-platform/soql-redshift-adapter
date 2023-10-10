package meta.entity

import jakarta.persistence.{Entity, GeneratedValue, Id, PersistenceUnit}

@Entity
class Dataset
(
  val nextCounterValue:Long,
  val localeName: String,
  val obfuscation_key: String,
  val resourceName: String,
  val latest_data_version: Long
){

}
