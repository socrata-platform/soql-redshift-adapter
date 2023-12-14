package com.socrata.db.meta.entity

import jakarta.persistence.{Column, Entity, Table}

@Entity
@Table(name = "dataset_map")
class DatasetInfo extends SocrataEntityBase {
  @Column(name = "next_counter_value")
  var nextCounterValue: Long = _
  @Column(name = "locale_name")
  var localeName: String = _
  @Column(name = "obfuscation_key")
  var obfuscationKey: Array[Byte] = _
  @Column(name = "resource_name")
  var resourceName: Option[String] = _
  @Column(name = "latest_data_version")
  var latestDataVersion: Long = _
}
