package com.socrata.store

import scala.util.Using
import com.socrata.soql.analyzer2._
import com.socrata.common.sqlizer.metatypes
import com.socrata.soql.types.{SoQLType}
import com.socrata.soql.sqlizer._

trait Schema[MT <: MetaTypes] {
  def update(table: MT#DatabaseTableNameImpl, column: MT#DatabaseColumnNameImpl)(ct: MT#ColumnType): Seq[UpdateCommand]
}

case class SchemaImpl(repProvider: Rep.Provider[metatypes.DatabaseNamesMetaTypes]) extends Schema[metatypes.DatabaseNamesMetaTypes] {
  def update(table: String, column: String)(ct: SoQLType): Seq[UpdateCommand] = {
    val rep = repProvider(ct)
    val physicalTypes = rep.physicalDatabaseTypes
    val names = rep.physicalDatabaseColumns(DatabaseColumnName(column))
    physicalTypes.zip(names).map { case (typ, name) =>
      UpdateCommand(table, name.toString(), typ.toString())
    }
  }
}

class UpdateCommand private(val underlying: String) extends AnyVal {
  def execute(conn: java.sql.Connection) =
    Using.resource(conn.createStatement()) { stmt =>
      stmt.executeUpdate(underlying)
    }
}
object UpdateCommand {
  def apply(table: String, columnName: String, columnType: String) = new UpdateCommand(f"ALTER TABLE ${table} ADD ${columnName} ${columnType}")
}
