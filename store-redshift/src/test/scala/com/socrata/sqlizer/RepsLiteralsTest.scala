package com.socrata.store.sqlizer

import org.joda.time.{DateTime, LocalDateTime, LocalDate, LocalTime}
import org.joda.time.format.{DateTimeFormat}

import com.socrata.common.sqlizer.metatypes._

import com.socrata.store._
import com.vividsolutions.jts.geom.{
  LineString,
  LinearRing,
  MultiLineString,
  MultiPoint,
  MultiPolygon,
  Point,
  Polygon,
  Coordinate,
  PrecisionModel
}

import com.socrata.soql.types._
import com.socrata.soql.analyzer2._

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._

import ZipExt._

@QuarkusTest
class RepsLiteralsTest extends TableCreationUtils {
  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  val repProvider = TestRepProvider
  val schema = SchemaImpl(repProvider)
  val rows = RowsImpl(repProvider)

  def testFails[T <: Throwable](literal: DatabaseNamesMetaTypes#ColumnValue)(expectedType: Class[T]) = {
    assertThrows(
      expectedType,
      () =>
        repProvider
          .reps(literal.typ).literal(LiteralValue[DatabaseNamesMetaTypes](literal)(AtomicPositionInfo.None))
    )
  }

  def test(@annotation.unused testName: String)(literal: DatabaseNamesMetaTypes#ColumnValue)(expected: String*) = {
    val rep = repProvider
      .reps(literal.typ)

    rep.literal(LiteralValue[DatabaseNamesMetaTypes](literal)(AtomicPositionInfo.None)).sqls.map(_.toString)
      .zipExact(expected.toList)
      .foreach {
        case (received, expected) => {
          assertEquals(expected, received)
          Utils.withTable(dataSource, "repsLiteral")("foo", "int") { (conn, tableName) =>
            schema.update(tableName, "testcol")(literal.typ).foreach(thing => thing.execute(conn))
            rows.update(tableName, "testcol")(literal).foreach(thing => thing.execute(conn))
          }
        }
      }
  }

  @Test
  def text(): Unit = {
    test("text")(SoQLText("here are some words"))("text 'here are some words'")
  }

  @Test
  def number(): Unit = {
    test("number")(SoQLNumber(new java.math.BigDecimal(22)))("22 :: decimal(30, 7)")
  }

  @Test
  def boolean(): Unit = {
    test("bool")(SoQLBoolean(false))("false")
    test("bool")(SoQLBoolean(true))("true")
  }

  val dateStr = "2021-06-13 18-14-23CST"
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-sszzz")

  @Test
  def fixedTimestamp(): Unit = {
    val dateTime: DateTime = formatter.parseDateTime(dateStr)
    test("fixed timestamp")(SoQLFixedTimestamp(dateTime))("timestamp with time zone '2021-06-13T23:14:23.000Z'")
  }

  @Test
  def floatingTimestamp(): Unit = {
    val dateTime: LocalDateTime = formatter.parseLocalDateTime(dateStr)
    test("floating timestamp")(SoQLFloatingTimestamp(dateTime))("timestamp without time zone '2021-06-13T18:14:23.000'")
  }

  @Test
  def date(): Unit = {
    val dateTime: LocalDate = formatter.parseLocalDate(dateStr)
    test("date")(SoQLDate(dateTime))("date '2021-06-13'")
  }

  @Test
  def time(): Unit = {
    val dateTime: LocalTime = formatter.parseLocalTime(dateStr)
    test("time")(SoQLTime(dateTime))("time without time zone '18:14:23.000'")
  }

  val precisionModel = new PrecisionModel()
  val coordinate = new Coordinate(100, 999)
  val pt = new Point(coordinate, precisionModel, Geo.defaultSRID)

  @Test
  def point(): Unit = {
    test("point")(SoQLPoint(pt))(
      "ST_GeomFromWKB('00000000014059000000000000408f380000000000', 4326)"
    )
  }

  @Test
  def multipoint(): Unit = {
    test("multipoint")(SoQLMultiPoint(new MultiPoint(Array(pt, pt, pt), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '00000000040000000300000000014059000000000000408f38000000000000000000014059000000000000408f38000000000000000000014059000000000000408f380000000000',
  4326
)"""
    )
  }

  val lineString = new LineString(Array(coordinate, coordinate), precisionModel, Geo.defaultSRID)

  @Test
  def line(): Unit = {
    test("line")(SoQLLine(lineString))(
      """ST_GeomFromWKB(
  '0000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)"""
    )
  }

  @Test
  def multiline(): Unit = {
    test("multiline")(SoQLMultiLine(new MultiLineString(Array(lineString, lineString), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '0000000005000000020000000002000000024059000000000000408f3800000000004059000000000000408f3800000000000000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)"""
    )
  }

  val poly = new Polygon(
    new LinearRing(Array(coordinate, coordinate, coordinate, coordinate), precisionModel, Geo.defaultSRID),
    precisionModel,
    Geo.defaultSRID
  )

  @Test
  def polygon(): Unit = {
    test("polygon")(SoQLPolygon(poly))(
      """ST_GeomFromWKB(
  '000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)"""
    )
  }

  @Test
  def multipolygon(): Unit = {
    test("polygon")(SoQLMultiPolygon(new MultiPolygon(Array(poly, poly, poly), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '000000000600000003000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)"""
    )
  }
}
