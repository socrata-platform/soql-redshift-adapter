package com.socrata.db.datasets

import com.socrata.common.db.meta.entity.Dataset

import scala.util._

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource

@jakarta.enterprise.context.ApplicationScoped
case class TableDeleter(@DataSource("store") store: AgroalDataSource) {
  def delete(
      dataset: Dataset
  ) =
    Using.resource(store.getConnection) { conn =>
      conn
        .getMetaData()
        .getTables(null, null, dataset.table, null)
        .next() match {
        case true =>
          Using.resource(conn.createStatement()) { stmt =>
            stmt.executeUpdate(
              s"drop table ${dataset.table}"
            )
          }
        case false => ()
      }
    }
}
