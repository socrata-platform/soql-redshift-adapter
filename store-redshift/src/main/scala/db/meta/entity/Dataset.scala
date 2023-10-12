package db.meta.entity

import jakarta.persistence.Entity

@Entity
class Dataset
(
  val nextCounterValue: Long,
  val localeName: String,
  val obfuscation_key: String,
  val resourceName: String,
  val latest_data_version: Long
) {

}
