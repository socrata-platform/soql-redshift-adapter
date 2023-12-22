package com.socrata.db.meta.entity

import jakarta.persistence._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase

@Entity
@Table(name = "datasets")
class Dataset extends PanacheEntityBase {

  /*




   The ID is deleted and reused every test run



   */

  @Id // use compound column instead of this silly id
  @GeneratedValue
  @Column(name = "system_id")
  var systemId: Long = _

  @Column(name = "obfuscation_key")
  var obfuscationKey: Array[Byte] = _

  @Column(name = "internal_name")
  var internalName: String = _

  @Column(name = "copy_number")
  var copyNumber: Long = _

  @Column(name = "table_name")
  var table: String = _

}

object Dataset {
  def apply(
      internalName: String,
      copyNumber: Long,
      obfuscationKey: Array[Byte]
  ): Dataset = {
    val out = new Dataset()
    out.obfuscationKey = obfuscationKey
    out.internalName = internalName
    out.copyNumber = copyNumber
    out
  }
}
