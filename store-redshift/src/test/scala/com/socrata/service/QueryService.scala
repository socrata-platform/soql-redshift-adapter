package com.socrata.service

import io.quarkus.logging.Log
import com.socrata.util.ResultSet.extractHeadOption
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped

import scala.util.Using

@ApplicationScoped
class QueryService(
    @DataSource("store")
    dataSource: AgroalDataSource
) {

  def getTableRowCount(tableName: String): Option[Long] = {
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.prepareStatement(s"""select count(*) from "$tableName";""")) { stmt =>
        Using.resource(stmt.executeQuery()) { resultSet =>
          val rowcount: Option[Long] = extractHeadOption(resultSet)(rs => rs.getLong(1));
          rowcount match {
            case Some(count) => Log.info(s"Table $tableName has $count records")
            case _ => Log.info(s"Unable to get record count for table $tableName")
          }
          rowcount
        }
      }
    }
  }

}
