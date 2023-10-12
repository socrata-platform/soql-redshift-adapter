package com.socrata

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}

import scala.util.Using

@DisplayName("Redshift insert tests")
@QuarkusTest
class InsertTest {

  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  @DisplayName("via JDBC sequential")
  @Test
  def jdbcSequential() = {
    Using(dataSource.getConnection){conn=>

    }.get
  }

  @DisplayName("via JDBC batch")
  @Test
  def jdbcBatch() = {
    Using(dataSource.getConnection) { conn =>

    }.get
  }

  @DisplayName("via S3")
  @Test
  def s3() = {

  }

}
