package com.socrata

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.redshift.AmazonRedshift
import com.amazonaws.services.s3.AmazonS3
import com.opencsv.{CSVParserBuilder, CSVReaderBuilder}
import com.socrata.service.InsertService
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test, Timeout}

import java.io.File
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Using

@DisplayName("Redshift insert tests")
@QuarkusTest
class InsertTest {

  @DataSource("store")
  @Inject var dataSource: AgroalDataSource = _

  @Inject var insertService: InsertService = _
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

  def readTestData(path: String) = {
    val reader = Source.fromURL(getClass.getResource(path))

    val parser = new CSVParserBuilder().withSeparator(',')
      .withIgnoreQuotations(false)
      .withIgnoreLeadingWhiteSpace(true)
      .withStrictQuotes(false)
      .build();

    val csvReader = new CSVReaderBuilder(reader.reader())
      .withSkipLines(1)
      .withCSVParser(parser)
      .build();
    csvReader
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
  }

  @DisplayName("100k rows via S3")
  @Test
  def insertS3100k(): Unit = {
    Timing.Timed {
      insertService.insertS3("staging-redshift-adapter","hdyn-4f6y",new File(getClass.getResource("/data/hdyn-4f6y/data.csv").toURI))
    } { elapsed =>
      println(s"100k rows via S3 took $elapsed")
    }
  }

}
