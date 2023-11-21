package com.socrata.store

import scala.util.Using
import com.socrata.soql.analyzer2._
import com.socrata.common.sqlizer.metatypes
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.soql.sqlizer._

trait Rows[MT <: MetaTypes] {
  def update(table: MT#DatabaseTableNameImpl, column: MT#DatabaseColumnNameImpl)(cv: MT#ColumnValue): Seq[InsertCommand]
}

case class RowsImpl(repProvider: Rep.Provider[metatypes.DatabaseNamesMetaTypes])(
  implicit val hasType: HasType[metatypes.DatabaseNamesMetaTypes#ColumnValue, metatypes.DatabaseNamesMetaTypes#ColumnType]) extends Rows[metatypes.DatabaseNamesMetaTypes] {


  def update(table: metatypes.AugmentedTableName, column: String)(cv: SoQLValue)= {
    val rep = repProvider(cv.typ)

    val names = rep.physicalDatabaseColumns(DatabaseColumnName(column))
    val updates = rep.literal(LiteralValue[metatypes.DatabaseNamesMetaTypes](cv)(AtomicPositionInfo.None)).sqls
    names.zip(updates).map { case (name, lit) =>
      InsertCommand(table.name, name.toString, lit.toString())
    }
  }
}

class InsertCommand private(val underlying: String) extends AnyVal {
  def execute(conn: java.sql.Connection) =
    Using.resource(conn.createStatement()) { stmt =>
      stmt.executeUpdate(underlying)
    }
}
object InsertCommand {
  def apply(table: String, columnName: String, value: String) = new InsertCommand(f"INSERT INTO $table ($columnName) values ($value)")
}
