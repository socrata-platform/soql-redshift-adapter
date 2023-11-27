package com.socrata.store.sqlizer

import scala.util._

import io.agroal.api.AgroalDataSource

object Utils {

  def withTable(dataSource: AgroalDataSource, tableName: String)(columnName: String, columnType: String)(fn: (java.sql.Connection, String) => Unit) =
    Using.resource(dataSource.getConnection) { conn =>
      try {
        Using.resource(conn.createStatement()) { stmt =>
          stmt.executeUpdate(
            s"""create table $tableName ($columnName $columnType)""")
        }
        fn(conn, tableName)
      } finally {
        Using.resource(conn.createStatement()) { stmt =>
          stmt.executeUpdate(
            s"""drop table if exists "$tableName"""")
        }
      }
    }
}
