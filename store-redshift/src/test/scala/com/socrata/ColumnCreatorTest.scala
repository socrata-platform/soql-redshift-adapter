package com.socrata.store.sqlizer

import scala.util._
import com.socrata.util.ResultSet
import com.socrata.common.sqlizer.metatypes._

import com.socrata.store._
import com.vividsolutions.jts.geom.{LineString, LinearRing, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, Coordinate, PrecisionModel}
import com.socrata.soql.parsing._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.interpolation._

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common._
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.soql.environment.Provenance


import io.quarkus.logging.Log
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._

import TableCreationTest.hasType

import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime, Period}
import org.joda.time.format.{DateTimeFormat}

import ZipExt._

@QuarkusTest
class ColumnCreatorTest {
  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  val repProvider = TableCreationTest.TestRepProvider

  val schema = SchemaImpl(repProvider)

  def test(`type`: DatabaseNamesMetaTypes#ColumnType)(expected: String*) =
    repProvider
      .reps(`type`).physicalDatabaseTypes.map(_.toString)
      .zipExact(expected.toList)
      .foreach { case (received, expected) => {
        assertEquals(expected, received)
        Utils.withTable(dataSource, "columnCreator")("foo", "int") { (conn, tableName) =>
          schema.update(tableName, "testcol")(`type`).foreach(_.execute(conn))
        }
      }}

  def testFails[T <: Throwable](`type`: DatabaseNamesMetaTypes#ColumnType)(expectedType: Class[T]) = {
    assertThrows(expectedType, () =>
      repProvider
        .reps(`type`).physicalDatabaseTypes)
  }

  @Test
  def text(): Unit = {
    test(SoQLText)("text")
  }

  @Test
  def number(): Unit = {
    test(SoQLNumber)("decimal(30, 7)")
  }

  @Test
  def boolean(): Unit = {
    test(SoQLBoolean)("boolean")
  }

  @Test
  def fixedTimestamp(): Unit = {
    test(SoQLFixedTimestamp)("timestamp with time zone")
  }

  @Test
  def floatingTimestamp(): Unit = {
    test(SoQLFloatingTimestamp)("timestamp without time zone")
  }

  @Test
  def date(): Unit = {
    test(SoQLDate)("date")
  }

  @Test
  def time(): Unit = {
    test(SoQLTime)("time without time zone")
  }

  @Test
  def json(): Unit = {
    test(SoQLJson)("super")
  }

  @Test
  def document(): Unit = {
    test(SoQLDocument)("super")
  }

  @Test
  def point(): Unit = {
    test(SoQLPoint)("geometry")
  }

  @Test
  def multipoint(): Unit = {
    test(SoQLMultiPoint)("geometry")
  }

  @Test
  def line(): Unit = {
    test(SoQLLine)("geometry")
  }

  @Test
  def multiline(): Unit = {
    test(SoQLMultiLine)("geometry")
  }

  @Test
  def polygon(): Unit = {
    test(SoQLPolygon)("geometry")
  }

  @Test
  def multipolygon(): Unit = {
    test(SoQLMultiPolygon)("geometry")
  }
}
