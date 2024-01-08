package com.socrata.store.handlers

import com.socrata.common.sqlizer._
import com.socrata.db.datasets._

import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._
import com.socrata.common.db.meta.entity.{Dataset, DatasetColumn}

trait Resync {
  def store(
      dataset: Dataset,
      columns: List[DatasetColumn],
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]
  ): Option[Long]
}

@ApplicationScoped
case class ResyncImpl(tableCreator: TableCreator) extends Resync {
  override def store(
      dataset: Dataset,
      columns: List[DatasetColumn],
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]
  ): Option[Long] = {

    tableCreator.create(SoQLSqlizer.repProvider(null, null, null))(
      dataset,
      columns.map(column => column -> schema(new ColumnId(column.columnId)))
    )
    None
  }
}
