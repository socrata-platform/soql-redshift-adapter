package com.socrata

import com.socrata.service.{InsertService, QueryService}
import io.quarkus.logging.Log
import com.socrata.util.TestData.readTestData
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, Disabled, DisplayName, Test}

import java.io.File
import scala.collection.JavaConverters._
import scala.util.Using

@Disabled
@DisplayName("Redshift insert tests")
@QuarkusTest
class InsertTest {

  @DataSource("store")
  @Inject var dataSource: AgroalDataSource = _

  @Inject var insertService: InsertService = _

  @Inject var queryService: QueryService = _

  @BeforeEach def beforeEach(): Unit = {
    Using.resource(dataSource.getConnection) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stmt.executeUpdate(
          """
            |create temporary table if not exists "100k"(
            |fiscal_year bigint not null,
            |department_name text not null,
            |supplier_name text not null,
            |description text not null,
            |procurement_eligible text not null,
            |cert_supplier text not null,
            |amount bigint not null,
            |cert_classification text not null
            |);
            |truncate table "100k";
            |""".stripMargin
        )
      }
    }
  }

  @DisplayName("100k rows via 1k batch, JDBC")
  @Test
  def insertJdbc100k1k(): Unit = {
    assume(queryService.getTableRowCount("100k").get == 0L)
    Timing.timed {
      insertService.insertJdbc(
        "100k",
        Array(
          "fiscal_year",
          "department_name",
          "supplier_name",
          "description",
          "procurement_eligible",
          "cert_supplier",
          "amount",
          "cert_classification"
        ),
        1000,
        readTestData("/data/100k/data.csv").iterator().asScala
      )
    } { elapsed =>
      Log.info(s"100k rows via 1k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("100k").get == 100000L)
  }

  @DisplayName("100k rows via 10k batch, JDBC")
  @Test
  def insertJdbc100k10k(): Unit = {
    assume(queryService.getTableRowCount("100k").get == 0L)
    Timing.timed {
      insertService.insertJdbc(
        "100k",
        Array(
          "fiscal_year",
          "department_name",
          "supplier_name",
          "description",
          "procurement_eligible",
          "cert_supplier",
          "amount",
          "cert_classification"
        ),
        10000,
        readTestData("/data/100k/data.csv").iterator().asScala
      )
    } { elapsed =>
      Log.info(s"100k rows via 10k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("100k").get == 100000L)
  }

  @DisplayName("100k rows via 100k batch, JDBC")
  @Test
  def insertJdbc100k100k(): Unit = {
    assume(queryService.getTableRowCount("100k").get == 0L)
    Timing.timed {
      insertService.insertJdbc(
        "100k",
        Array(
          "fiscal_year",
          "department_name",
          "supplier_name",
          "description",
          "procurement_eligible",
          "cert_supplier",
          "amount",
          "cert_classification"
        ),
        100000,
        readTestData("/data/100k/data.csv").iterator().asScala
      )
    } { elapsed =>
      Log.info(s"100k rows via 100k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("100k").get == 100000L)
  }

  @DisplayName("100k rows via S3")
  @Test
  def insertS3100k(): Unit = {
    assume(queryService.getTableRowCount("100k").get == 0L)
    Timing.timed {
      insertService.insertS3(
        "staging-redshift-adapter",
        "100k",
        new File(getClass.getResource("/data/100k/data.csv").toURI)
      )
    } { elapsed =>
      Log.info(s"100k rows via S3 took $elapsed")
    }
    assert(queryService.getTableRowCount("100k").get == 100000L)
  }

}
