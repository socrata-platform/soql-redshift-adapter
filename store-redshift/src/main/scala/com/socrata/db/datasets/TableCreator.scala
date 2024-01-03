package com.socrata.db.datasets

import scala.util._
import com.socrata.soql.sqlizer._
import com.socrata.common.sqlizer._
import com.socrata.db.meta.entity._
import com.socrata.db.Exists
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._

@jakarta.enterprise.context.ApplicationScoped
case class TableCreator(@DataSource("store") store: AgroalDataSource) {
  def create(repProvider: SoQLRepProviderRedshift[metatypes.DatabaseNamesMetaTypes])(
      dataset: Dataset,
      columns: List[(DatasetColumn, ColumnInfo[SoQLType])],
      blobUrl: String): Exists.Exists[String] = {
    val dbColumns: List[(String, String)] = columns.flatMap {
      case (dbColumn, column) => {
        val rep = repProvider.reps(column.typ)
        rep.physicalDatabaseColumns(DatabaseColumnName(dbColumn.columnName))
          .map(_.toString)
          .zip(rep.physicalDatabaseTypes.map(_.toString)).toList
      }
    }

    val dbColumnFragment = dbColumns.map({ case (name, typ) => s"$name $typ" }).mkString(", ")

    val sql = s"""create table ${dataset.table} ($dbColumnFragment)"""

    Using.resource(store.getConnection) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stmt.executeUpdate(
          sql
        )
      }

      Exists.Inserted(dataset.table)
    }
  }
}
