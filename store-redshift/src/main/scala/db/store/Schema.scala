package com.socrata.store

import com.socrata.soql.analyzer2._
import com.socrata.common.sqlizer.metatypes
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}


trait Schema[MT <: MetaTypes] {
  def update(table: MT#DatabaseTableNameImpl, column: MT#DatabaseColumnNameImpl)(ct: MT#ColumnType): UpdateCommand
  def updates(table: MT#DatabaseTableNameImpl, column: MT#DatabaseColumnNameImpl)(cts: Seq[MT#ColumnType]): Seq[UpdateCommand] = cts.map(update(table, column))
}

case class SchemaImpl() extends Schema[metatypes.DatabaseNamesMetaTypes] {
  def update(table: metatypes.AugmentedTableName, column: String)(ct: SoQLType): UpdateCommand = {
    UpdateCommand(table.name, column, "") // do this
  }
}

class UpdateCommand private(val underlying: String) extends AnyVal
object UpdateCommand {
  def apply(table: String, columnName: String, columnType: String) = new UpdateCommand(f"ALTER TABLE ${table} ADD ${columnName} ${columnType}")
}
