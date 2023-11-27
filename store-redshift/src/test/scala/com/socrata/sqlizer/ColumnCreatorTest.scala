package com.socrata.store.sqlizer

import com.socrata.common.sqlizer.metatypes._

import com.socrata.store._
import com.socrata.soql.types._

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;

import ZipExt._

@QuarkusTest
class ColumnCreatorTest extends TableCreationUtils {
  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  val repProvider = TestRepProvider

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
