package com.socrata.common.db.meta.entity

import com.socrata.common.names.ColumnNames
import com.socrata.datacoordinator.secondary._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase
import jakarta.persistence._

@Entity
@Table(name = "columns")
class DatasetColumn extends PanacheEntityBase {

  @Id
  @GeneratedValue
  @Column(name = "system_id")
  var systemId: Long = _

  @Column(name = "dataset_id")
  var datasetId: Long = _

  @Column(name = "column_id")
  var columnId: Long = _

  @Column(name = "column_name")
  var columnName: String = _

  override def toString() = s"""Column(
    system_id: ${systemId}
    dataset_id: ${datasetId}
    column_id: ${columnId}
    column_name: ${columnName}
)
"""
}

object DatasetColumn {
  def apply(
      dataset: Dataset,
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      columnInfo: ColumnInfo[_]
  ): DatasetColumn = {
    val out = new DatasetColumn()
    out.datasetId = dataset.systemId
    out.columnId = columnInfo.systemId.underlying
    out.columnName = ColumnNames.from(columnInfo)
    out
  }

  def update(updated: DatasetColumn, copyFrom: DatasetColumn) = {
    updated.columnName = copyFrom.columnName
    updated
  }

}
/*
 create table columns (system_id bigint primary key, dataset_id bigint, column_id bigint, column_name varchar)
 */
