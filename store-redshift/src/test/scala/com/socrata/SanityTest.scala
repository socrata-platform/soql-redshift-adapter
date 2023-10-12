package com.socrata

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

  @DisplayName("Connect to redshift jdbc")
  @Test
  def connectToRedshiftJdbc() = {
    Using.resource(dataSource.getConnection) { conn =>
      val databaseName = conn.getMetaData.getDatabaseProductName;
      println(s"Database name: '$databaseName'")
      assert(databaseName=="Redshift")
    }
  }

}
