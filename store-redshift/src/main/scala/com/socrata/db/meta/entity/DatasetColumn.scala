package com.socrata.db.meta.entity

import com.socrata.datacoordinator.secondary._
import com.socrata.store.names
import jakarta.persistence._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase

// try to make these not VARS

@Entity
@Table(name = "columns")
class DatasetColumn extends PanacheEntityBase {

  @Id // use compound column instead of this silly id
  @GeneratedValue
  @Column(name = "system_id")
  var systemId: Long = _

  @Column(name = "internal_name")
  var internalName: String = _

  @Column(name = "copy_number")
  var copyNumber: Long = _

  @Column(name = "column_id")
  var columnId: Long = _

  @Column(name = "column_name")
  var columnName: String = _
}

object DatasetColumn {
  def apply(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      columnInfo: ColumnInfo[_]
  ): DatasetColumn = {
    val out = new DatasetColumn()
    out.internalName = datasetInfo.internalName
    out.copyNumber = copyInfo.copyNumber
    out.columnId = columnInfo.systemId.underlying
    out.columnName = names.ColumnNames.from(columnInfo)
    out
  }
}
