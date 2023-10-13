package com.socrata

import com.socrata.service.{InsertService, QueryService}
import com.socrata.util.TestData.readTestData
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test}

import java.io.File
import scala.collection.JavaConversions._
import scala.util.Using

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

  @DisplayName("100k rows via 1k batch, JDBC")
  @Test
  def insertJdbc100k1k(): Unit = {
    Timing.Timed {
      insertService.insertJdbc(
        "hdyn-4f6y",
        Array("fiscal_year", "department_name", "supplier_name", "description", "procurement_eligible", "cert_supplier", "amount", "cert_classification"),
        1000,
        readTestData("/data/hdyn-4f6y/data.csv").iterator()
      )
    } { elapsed =>
      println(s"100k rows via 1k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("hdyn-4f6y").get == 100000L)
  }

  @DisplayName("100k rows via 10k batch, JDBC")
  @Test
  def insertJdbc100k10k(): Unit = {
    Timing.Timed {
      insertService.insertJdbc(
        "hdyn-4f6y",
        Array("fiscal_year", "department_name", "supplier_name", "description", "procurement_eligible", "cert_supplier", "amount", "cert_classification"),
        10000,
        readTestData("/data/hdyn-4f6y/data.csv").iterator()
      )
    } { elapsed =>
      println(s"100k rows via 10k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("hdyn-4f6y").get == 100000L)
  }

  @DisplayName("100k rows via 100k batch, JDBC")
  @Test
  def insertJdbc100k100k(): Unit = {
    Timing.Timed {
      insertService.insertJdbc(
        "hdyn-4f6y",
        Array("fiscal_year", "department_name", "supplier_name", "description", "procurement_eligible", "cert_supplier", "amount", "cert_classification"),
        100000,
        readTestData("/data/hdyn-4f6y/data.csv").iterator()
      )
    } { elapsed =>
      println(s"100k rows via 100k batch, JDBC took $elapsed")
    }
    assert(queryService.getTableRowCount("hdyn-4f6y").get == 100000L)
  }

  @DisplayName("100k rows via S3")
  @Test
  def insertS3100k(): Unit = {
    Timing.Timed {
      insertService.insertS3("staging-redshift-adapter", "hdyn-4f6y", new File(getClass.getResource("/data/hdyn-4f6y/data.csv").toURI))
    } { elapsed =>
      println(s"100k rows via S3 took $elapsed")
    }
    assert(queryService.getTableRowCount("hdyn-4f6y").get == 100000L)
  }

}
