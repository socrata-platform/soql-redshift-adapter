package com.socrata.db.meta.entity

import jakarta.persistence._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase

@Entity
@Table(name = "datasets")
class Dataset extends PanacheEntityBase {

  @Id
  @GeneratedValue
  @Column(name = "system_id")
  var systemId: Long = _

  @Column(name = "obfuscation_key")
  var obfuscationKey: Array[Byte] = _

  @Column(name = "resource_name")
  var resourceName: String = _

  @Column(name = "copy_number")
  var copyNumber: Long = _

  @Column(name = "table_name")
  var table: String = _

}
