package com.socrata.db.meta.entity

import com.socrata.datacoordinator.secondary._
import com.socrata.store.names
import jakarta.persistence._
import io.quarkus.hibernate.orm.panache.PanacheEntityBase

// try to make these not VARS

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
    out.columnName = names.ColumnNames.from(columnInfo)
    out
  }
}
/*
 create table columns (system_id bigint primary key, dataset_id bigint, column_id bigint, column_name varchar)
 */
