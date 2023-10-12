package com.socrata

import scala.collection.JavaConversions._
import com.opencsv.{CSVParserBuilder, CSVReader, CSVReaderBuilder}
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test, Timeout}

import java.util.concurrent.TimeUnit
import scala.io.{BufferedSource, Source}
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

  def readTestData(path:String)={
    val reader = Source.fromURL(getClass.getResource(path))

    val parser = new CSVParserBuilder()
      .withSeparator(',')
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
  @Test
  def jdbcSequential():Unit = {
    val data = readTestData("/data/hdyn-4f6y/data.csv")
    val batchSize = 10000
    Timing.Timed {
      Using.resource(dataSource.getConnection) { conn =>
        Using.resource(conn.prepareStatement("""insert into "hdyn-4f6y"(fiscal_year, department_name, supplier_name, description, procurement_eligible, cert_supplier, amount, cert_classification) values(?,?,?,?,?,?,?,?);""".stripMargin)) { stmt =>
          var count = 0
          for (elem <- data.iterator()) {
            stmt.setLong(1, elem(0).trim.toLong)
            stmt.setString(2, elem(1).trim)
            stmt.setString(3, elem(2).trim)
            stmt.setString(4, elem(3).trim)
            stmt.setString(5, elem(4).trim)
            stmt.setString(6, elem(5).trim)
            stmt.setLong(7, elem(6).trim.toLong)
            stmt.setString(8, elem(7).trim)
            stmt.addBatch()
            count+=1
            if (count%batchSize==0){
              stmt.executeUpdate()
            }
          }
          stmt.executeUpdate()
        }

      }
    } { elapsed =>
      println(s"Inserting via JDBC sequential took $elapsed")
    }
  }

  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  @DisplayName("100k rows all batched, JDBC")
  @Test
  def jdbcBatch(): Unit = {
    val data = readTestData("/data/hdyn-4f6y/data.csv")

    Timing.Timed {
      Using.resource(dataSource.getConnection) { conn =>
        Using.resource(conn.prepareStatement("""insert into "hdyn-4f6y"(fiscal_year, department_name, supplier_name, description, procurement_eligible, cert_supplier, amount, cert_classification) values(?,?,?,?,?,?,?,?);""".stripMargin)) { stmt =>
          for (elem <- data.iterator()) {
            stmt.setLong(1, elem(0).trim.toLong)
            stmt.setString(2, elem(1).trim)
            stmt.setString(3, elem(2).trim)
            stmt.setString(4, elem(3).trim)
            stmt.setString(5, elem(4).trim)
            stmt.setString(6, elem(5).trim)
            stmt.setLong(7, elem(6).trim.toLong)
            stmt.setString(8, elem(7).trim)
            stmt.addBatch()
          }
          stmt.executeUpdate()
        }

      }
    } { elapsed =>
      println(s"Inserting via JDBC sequential took $elapsed")
    }
  }

  @DisplayName("via S3")
  @Test
  def s3() = {

  }

}
