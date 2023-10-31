package com.socrata

import com.socrata.config.RedshiftSecondaryConfig
import io.quarkus.logging.Log
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}

import scala.util.Using

@DisplayName("Sanity Tests")
@QuarkusTest
class SanityTest() {
  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  @Inject
  var redshiftSecondaryConfig: RedshiftSecondaryConfig = _

  @DisplayName("Connect to redshift jdbc")
  @Test
  def connectToRedshiftJdbc() = {
    Using.resource(dataSource.getConnection) { conn =>
      val databaseName = conn.getMetaData.getDatabaseProductName;
      Log.info(s"Database name: '$databaseName'")
      assert(databaseName == "Redshift")
    }
  }

}
