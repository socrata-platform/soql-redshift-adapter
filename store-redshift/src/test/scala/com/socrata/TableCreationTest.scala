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

object TableCreationTest {
  object ProvenanceMapper extends types.ProvenanceMapper[DatabaseNamesMetaTypes] {
    def toProvenance(dtn: types.DatabaseTableName[DatabaseNamesMetaTypes]): Provenance = {
      val DatabaseTableName(name) = dtn
      Provenance(name)
    }

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseNamesMetaTypes] = {
      val Provenance(name) = prov
      DatabaseTableName(name)
    }
  }

  object TestNamespaces extends SqlNamespaces[DatabaseNamesMetaTypes] {
    override def rawDatabaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      name
    }

    override def rawDatabaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(name) = dcn
      name
    }

    override def gensymPrefix: String = "g"
    protected override def idxPrefix: String ="idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
  }

  val TestFuncallSqlizer = new SoQLFunctionSqlizerRedshift[DatabaseNamesMetaTypes]

  val TestSqlizer = new Sqlizer[DatabaseNamesMetaTypes](
    TestFuncallSqlizer,
    new RedshiftExprSqlFactory[DatabaseNamesMetaTypes],
    TestNamespaces,
    new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true),
    ProvenanceMapper,
    _ => false,
    (sqlizer, physicalTableFor, extraContext) =>
    new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
      extraContext.cryptProviderProvider,
      sqlizer.namespace,
      sqlizer.exprSqlFactory
    ) {
      override def mkStringLiteral(s: String) = Doc(extraContext.escapeString(s))
    }
  )

  def extraContext = new SoQLExtraContext(
    Map.empty,
    _ => Some(obfuscation.CryptProvider.zeros),
    s => s"'$s'"
  )

  val TestRepProvider = new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
    extraContext.cryptProviderProvider,
    TestSqlizer.namespace,
    TestSqlizer.exprSqlFactory
  ) {
    override def mkStringLiteral(s: String) = Doc(extraContext.escapeString(s))
  }

  implicit val hasType: HasType[DatabaseNamesMetaTypes#ColumnValue, DatabaseNamesMetaTypes#ColumnType]  = new HasType[DatabaseNamesMetaTypes#ColumnValue, DatabaseNamesMetaTypes#ColumnType] {
      def typeOf(cv: DatabaseNamesMetaTypes#ColumnValue): DatabaseNamesMetaTypes#ColumnType = cv.typ
    }
}

@QuarkusTest
class ColumnCreator {
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

/*
do compound types
do ID and version

test various soql commands that may fail due to super stuff
 */
