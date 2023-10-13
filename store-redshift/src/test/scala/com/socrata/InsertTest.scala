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

import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Using

@DisplayName("Redshift insert tests")
@QuarkusTest class InsertTest {

  @DataSource("store")
  @Inject var dataSource: AgroalDataSource = _

  @Inject var s3: AmazonS3 = _

  @Inject var redshift: AmazonRedshift = _

  @Inject var awsCredentials: AWSCredentials = _

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

  @DisplayName("100k rows via 10k batch, JDBC")
  @Test def insertJdbc100k10k(): Unit = {
    Timing.Timed {
      insertService.insert(
        "hdyn-4f6y",
        Array("fiscal_year", "department_name", "supplier_name", "description", "procurement_eligible", "cert_supplier", "amount", "cert_classification"),
        10000,
        readTestData("/data/hdyn-4f6y/data.csv").iterator()
      )
    } { elapsed =>
      println(s"100k rows via 10k batch, JDBC took $elapsed")
    }
  }

  @DisplayName("100k rows all batched, JDBC")
  @Test def insertJdbc100k100k(): Unit = {
    Timing.Timed {
      insertService.insert(
        "hdyn-4f6y",
        Array("fiscal_year", "department_name", "supplier_name", "description", "procurement_eligible", "cert_supplier", "amount", "cert_classification"),
        100000,
        readTestData("/data/hdyn-4f6y/data.csv").iterator()
      )
    } { elapsed =>
      println(s"100k rows all batched, JDBC took $elapsed")
    }
  }

  @DisplayName("100k rows, S3")
//  @Test
  def insertS3100k(): Unit = {
    val bucket = "staging-redshift-adapter"
    val table = "hdyn-4f6y"
    val filename = "hdyn-4f6y.csv"
    s3.deleteObject(bucket,filename)
    Timing.Timed {
      Using.resource(dataSource.getConnection) { conn =>
        Using.resource(conn.prepareStatement(
          s"""
            |copy "$table"
            |from 's3://$bucket/$filename'
            |access_key_id '${awsCredentials.getAWSAccessKeyId}'
            |secret_access_key '${awsCredentials.getAWSSecretKey}'
            |format as csv
            |ignoreheader 1;
            |""".stripMargin)) { stmt =>
          stmt.executeUpdate()
        }

      }
    } { elapsed =>
      println(s"100k rows, S3 took $elapsed")
    }
  }

}
