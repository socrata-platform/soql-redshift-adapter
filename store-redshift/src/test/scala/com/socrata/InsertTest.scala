package com.socrata

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat, QUOTE_ALL, QUOTE_NONNUMERIC, Quoting}
import com.socrata.util.Timing
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test}

import scala.io.Source
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
    Timing.Timed {
      Using.resource(dataSource.getConnection) { conn =>
        for (elem <- CSVReader.open(Source.fromURL(getClass.getResource("/data/hdyn-4f6y/data.csv"))).iteratorWithHeaders) {
          Using.resource(conn.prepareStatement("""insert into "hdyn-4f6y"(fiscal_year, department_name, supplier_name, description, procurement_eligible, cert_supplier, amount, cert_classification) values(?,?,?,?,?,?,?,?);""".stripMargin)){stmt=>
            try{
              stmt.setLong(1, elem("fiscal_year").toLong)
              stmt.setString(2, elem("department_name"))
              stmt.setString(3, elem("supplier_name"))
              stmt.setString(4, elem("description"))
              stmt.setString(5, elem("procurement_eligible"))
              stmt.setString(6, elem("cert_supplier"))
              stmt.setLong(7, elem("amount").toLong)
              stmt.setString(8, elem("cert_classification"))
            }catch {
              case e=> println(s"Error: elem:'$elem', err:'$e'")
            }
            stmt.executeUpdate()
          }
        }
      }
    }{elapsed=>
      println(s"Inserting via JDBC sequential took $elapsed")
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
