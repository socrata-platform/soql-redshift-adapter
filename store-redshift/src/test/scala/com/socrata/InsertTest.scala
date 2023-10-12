package com.socrata

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test}

import scala.util.Using

@DisplayName("Redshift insert tests")
@QuarkusTest
class InsertTest {

  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  @BeforeEach
  def beforeEach(): Unit = {
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stmt.executeUpdate(
          """
            |create temporary table if not exists "hdyn-4f6y"(
            |fiscal_year bigint not null,
            |department_name text not null,
            |supplier_name text not null,
            |description text not null,
            |procurement_eligible text not null,
            |cert_supplier text not null,
            |amount bigint not null,
            |cert_classification text not null
            |);
            |truncate table "hdyn-4f6y";
            |""".stripMargin)
      }
    }
  }

  @DisplayName("via JDBC sequential")
  @Test
  def jdbcSequential() = {
    Using.resource(dataSource.getConnection) { conn =>

    }
  }

  @DisplayName("via JDBC batch")
  @Test
  def jdbcBatch() = {
    Using.resource(dataSource.getConnection) { conn =>

    }
  }

  @DisplayName("via S3")
  @Test
  def s3() = {

  }

}
