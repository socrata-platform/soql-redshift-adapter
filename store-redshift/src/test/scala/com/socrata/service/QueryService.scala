package com.socrata.service

import com.socrata.util.ResultSet.extract
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped

import scala.util.Using

@ApplicationScoped
class QueryService
(
  @DataSource("store")
  dataSource: AgroalDataSource,
) {

  def getTableRowCount(tableName: String): Option[Long] = {
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.prepareStatement(s"""select count(*) from "$tableName";""")) { stmt =>
        Using.resource(stmt.executeQuery()) { resultSet =>
          val rowcount: Option[Long] = extract(resultSet)(rs => rs.getLong(1)).headOption;
          rowcount match {
            case Some(count) => println(s"Table $tableName has $count records")
            case _ => println(s"Unable to get record count for table $tableName")
          }
          rowcount
        }
      }
    }
  }

}
