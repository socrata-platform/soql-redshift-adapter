package com.socrata.common.db.meta.entity

import com.socrata.common.names.TableName
import com.socrata.datacoordinator.secondary._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase
import jakarta.persistence._

@Entity
@Table(name = "datasets")
class Dataset extends PanacheEntityBase {

  @Id
  @GeneratedValue
  @Column(name = "system_id")
  var systemId: Long = _

  @Column(name = "obfuscation_key")
  var obfuscationKey: Array[Byte] = _

  @Column(name = "internal_name")
  var internalName: String = _

  @Column(name = "copy_number")
  var copyNumber: Long = _

  @Column(name = "published")
  var published: Boolean = _

  @Column(name = "table_name")
  var table: String = _

  override def toString() = s"""Dataset(
    system_id: ${systemId}
    obfuscation_key: ${obfuscationKey}
    internal_name: ${internalName}
    copy_number: ${copyNumber}
    published: ${published}
    table: ${table}
)
"""

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
    out.published = copyInfo.lifecycleStage == LifecycleStage.Published
    out.table = TableName.from(datasetInfo)
    out
  }

  def update(updated: Dataset, copyFrom: Dataset) = {
    updated.obfuscationKey = copyFrom.obfuscationKey
    updated.published = copyFrom.published
    updated.table = copyFrom.table
    updated
  }
}

/*
 create table datasets (system_id bigint primary key, obfuscation_key bytea, internal_name text, copy_number bigint, published bool, table_name varchar)
 */
