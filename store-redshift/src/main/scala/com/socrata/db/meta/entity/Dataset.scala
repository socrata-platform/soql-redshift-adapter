package com.socrata.db.meta.entity

import com.socrata.datacoordinator.secondary._
import jakarta.persistence._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase

@Entity
@Table(name = "datasets")
class Dataset extends PanacheEntityBase {

// try to make these not VARS

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
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo
  ): Dataset = {
    val out = new Dataset()
    out.obfuscationKey = datasetInfo.obfuscationKey
    out.internalName = datasetInfo.internalName
    out.copyNumber = copyInfo.copyNumber
    out
  }
}
