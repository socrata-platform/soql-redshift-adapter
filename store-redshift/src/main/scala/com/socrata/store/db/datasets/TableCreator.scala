package com.socrata.db.datasets

import com.socrata.common.db.Exists
import com.socrata.common.db.meta.entity.{Dataset, DatasetColumn}

import scala.util._
import com.socrata.common.sqlizer._

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._

object TableCreator {
  type ColumnName = String
  type ColumnType = String
}

@jakarta.enterprise.context.ApplicationScoped
case class TableCreator(@DataSource("store") store: AgroalDataSource) {
  def create(
      repProvider: SoQLRepProviderRedshift[metatypes.DatabaseNamesMetaTypes]
  )(
      dataset: Dataset,
      columns: List[(DatasetColumn, ColumnInfo[SoQLType])]
  ): Exists.Exists[String] =
    Using.resource(store.getConnection) { conn =>
      conn
        .getMetaData()
        .getTables(null, null, dataset.table, null)
        .next() match {
        case true =>
          throw new IllegalStateException(
            s"dataset ${dataset} already exists in redshift"
          )
        case false => {
          val dbColumns
              : List[(TableCreator.ColumnName, TableCreator.ColumnType)] =
            columns.flatMap {
              case (dbColumn, column) => {
                val rep = repProvider.reps(column.typ)
                rep
                  .physicalDatabaseColumns(
                    DatabaseColumnName(dbColumn.columnName)
                  )
                  .map(_.toString)
                  .zip(rep.physicalDatabaseTypes.map(_.toString))
                  .toList
              }
            }

          val dbColumnFragment =
            dbColumns.map({ case (name, typ) => s"$name $typ" }).mkString(", ")

          val sql = s"create table ${dataset.table} ($dbColumnFragment)"
          Using.resource(conn.createStatement()) { stmt =>
            stmt.executeUpdate(
              sql
            )
          }
          Exists.Inserted(dataset.table)
        }
      }
    }
}
