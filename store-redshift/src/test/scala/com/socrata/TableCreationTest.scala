package com.socrata.store.sqlizer

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

object ZipExt {
  implicit class ZipUtils[T](seq: Seq[T]) {
    def zipExact[R](other: Seq[R]): Seq[(T, R)] = {
      if(seq.length == other.length) {
        seq.zip(other)
      } else fail(s"""You cannot pass...${seq} and ${other} are different lengths
The dark fire will not avail you, flame of Udûn. """)
    }
  }
}

import ZipExt._


object TableCreationTest {
  final abstract class TestMT extends MetaTypes with metatypes.SoQLMetaTypesExt {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Int
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  object ProvenanceMapper extends types.ProvenanceMapper[TestMT] {
    def toProvenance(dtn: types.DatabaseTableName[TestMT]): Provenance = {
      val DatabaseTableName(name) = dtn
      Provenance(name)
    }

    def fromProvenance(prov: Provenance): types.DatabaseTableName[TestMT] = {
      val Provenance(name) = prov
      DatabaseTableName(name)
    }
  }

  object TestNamespaces extends SqlNamespaces[TestMT] {
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

  val TestFuncallSqlizer = new SoQLFunctionSqlizerRedshift[TestMT]

  val TestSqlizer = new Sqlizer[TestMT](
    TestFuncallSqlizer,
    new RedshiftExprSqlFactory[TestMT],
    TestNamespaces,
    new SoQLRewriteSearch[TestMT](searchBeforeQuery = true),
    ProvenanceMapper,
    _ => false,
    (sqlizer, physicalTableFor, extraContext) =>
    new SoQLRepProviderRedshift[TestMT](
      extraContext.cryptProviderProvider,
      sqlizer.exprSqlFactory,
      sqlizer.namespace,
      sqlizer.toProvenance,
      sqlizer.isRollup,
      Map.empty,
      physicalTableFor
    ) {
      override def mkStringLiteral(s: String) = Doc(extraContext.escapeString(s))
    }
  )

  def extraContext = new SoQLExtraContext(
    Map.empty,
    _ => Some(obfuscation.CryptProvider.zeros),
    Map.empty,
    s => s"'$s'"
  )

  val TestRepProvider = new SoQLRepProviderRedshift[TestMT](
    extraContext.cryptProviderProvider,
    TestSqlizer.exprSqlFactory,
    TestSqlizer.namespace,
    TestSqlizer.toProvenance,
    TestSqlizer.isRollup,
    Map.empty,
    Map.empty
  ) {
    override def mkStringLiteral(s: String) = Doc(extraContext.escapeString(s))
  }

  implicit val hasType: HasType[TableCreationTest.TestMT#ColumnValue, TableCreationTest.TestMT#ColumnType]  = new HasType[TableCreationTest.TestMT#ColumnValue, TableCreationTest.TestMT#ColumnType] {
      def typeOf(cv: TableCreationTest.TestMT#ColumnValue): TableCreationTest.TestMT#ColumnType = cv.typ
    }

}

class TableCreationTest  {

  type TestMT = TableCreationTest.TestMT

  val sqlizer = TableCreationTest.TestSqlizer
  val funcallSqlizer = TableCreationTest.TestFuncallSqlizer
  val repProvider = TableCreationTest.TestRepProvider
}

class RepsLiterals {
  val repProvider = TableCreationTest.TestRepProvider
  type TestMT = TableCreationTest.TestMT

  def testFails[T <: Throwable](literal: TestMT#ColumnValue)(expectedType: Class[T]) = {
    assertThrows(expectedType, () =>
      repProvider
        .reps(literal.typ).literal(LiteralValue[TestMT](literal)(AtomicPositionInfo.None))
    )
  }

  def test(literal: TestMT#ColumnValue)(expected: String*) =
    repProvider
      .reps(literal.typ).literal(LiteralValue[TestMT](literal)(AtomicPositionInfo.None)).sqls.map(_.toString)
      .zipExact(expected.toList)
      .foreach { case (received, expected) => assertEquals(expected, received)}

  @Test
  def soqlText(): Unit = {
    test(SoQLText("here are some words"))("text 'here are some words'")
  }


  @Test
  def soqlNumber(): Unit = {
    test(SoQLNumber(new java.math.BigDecimal(22)))("22 :: decimal(30, 7)")
  }

  @Test
  def SoQLBoolean(): Unit = {
    test(new SoQLBoolean(false))("false")
    test(new SoQLBoolean(true))("true")
  }

  val dateStr = "2021-06-13 18-14-23CST"
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-sszzz")

  @Test
  def soqlFixedTimestamp(): Unit = {
    val dateTime: DateTime = formatter.parseDateTime(dateStr)
    test(SoQLFixedTimestamp(dateTime))("timestamp with time zone '2021-06-13T23:14:23.000Z'")
  }

  @Test
  def SoQLFloatingTimestamp(): Unit = {
    val dateTime: LocalDateTime = formatter.parseLocalDateTime(dateStr)
    test(new SoQLFloatingTimestamp(dateTime))("timestamp without time zone '2021-06-13T18:14:23.000'")
  }

  @Test
  def SoQLDate(): Unit = {
    val dateTime: LocalDate = formatter.parseLocalDate(dateStr)
    test(new SoQLDate(dateTime))("date '2021-06-13'")
  }

  @Test
  def SoQLTime(): Unit = {
    val dateTime: LocalTime = formatter.parseLocalTime(dateStr)
    test(new SoQLTime(dateTime))("time without time zone '18:14:23.000'")
  }

  // probably broken -- think about this
  @Test
  def SoQLJson(): Unit = {
    test(new SoQLJson(JNumber(2)))("JSON_PARSE(2)")
    test(new SoQLJson(JNumber(2.18)))("JSON_PARSE(2.18)")
    test(new SoQLJson(j"""{"foo": 22}"""))("""JSON_PARSE('{"foo":22}')""")
    test(new SoQLJson(JNull))("JSON_PARSE(null)")
    test(new SoQLJson(JArray(Seq(JNumber(2), JString("foo")))))("""JSON_PARSE('[2,"foo"]')""")
  }

  @Test
  def SoQLDocument(): Unit = {
    testFails(new SoQLDocument("", None, None))(classOf[NotImplementedError])
  }

  @Test
  def SoQLInterval(): Unit = {
    test(new SoQLInterval(new Period(1, 2, 3, 4, 5, 6, 7, 8)))("interval '1 years, 2 months, 3 weeks, 4 days, 5 hours, 6 minutes, 7 seconds'")
  }

  val precisionModel = new PrecisionModel()
  val coordinate = new Coordinate(100, 999)
  val point = new Point(coordinate, precisionModel, Geo.defaultSRID)

  @Test
  def SoQLPoint(): Unit = {
    test(new SoQLPoint(point))(
      "ST_GeomFromWKB('00000000014059000000000000408f380000000000', 4326)"
    )
  }

  @Test
  def SoQLMultiPoint(): Unit = {
    test(new SoQLMultiPoint(new MultiPoint(Array(point, point, point), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '00000000040000000300000000014059000000000000408f38000000000000000000014059000000000000408f38000000000000000000014059000000000000408f380000000000',
  4326
)""")


  }

  val lineString = new LineString(Array(coordinate, coordinate), precisionModel, Geo.defaultSRID)

  @Test
  def SoQLLine(): Unit = {
    test(new SoQLLine(lineString))(
      """ST_GeomFromWKB(
  '0000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  @Test
  def SoQLMultiLine(): Unit = {
    test(new SoQLMultiLine(new MultiLineString(Array(lineString, lineString), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '0000000005000000020000000002000000024059000000000000408f3800000000004059000000000000408f3800000000000000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  val polygon = new Polygon(new LinearRing(Array(coordinate, coordinate, coordinate, coordinate), precisionModel, Geo.defaultSRID), precisionModel, Geo.defaultSRID)

  @Test
  def SoQLPolygon(): Unit = {
    test(new SoQLPolygon(polygon))(
      """ST_GeomFromWKB(
  '000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  @Test
  def SoQLMultiPolygon(): Unit = {
    test(new SoQLMultiPolygon(new MultiPolygon(Array(polygon, polygon, polygon), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '000000000600000003000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }


  // untested
  @Test
  def SoQLPhone(): Unit = {
    // test(new SoQLPhone(Some("325-555-5555"), Some("home")))(
    //   """JSON_PARSE('[text "325-555-5555",text "home"]')""" // not right. Should be array of strings. the text inside will blow everything up
    // )
  }

}

/*
 1. talk to Dalia about geometry things (they're wrong function calls. Let's make sure they produce the right types too)
 test all geom and interval things
 test column create commands (compressedSubCols and stuff)
 test compression of bag of columns into a super

 make tests construct real tables and run queries against them
 make tests construct real literals and verify they work


make sure we can read these literals when written to a table. make sure we can read back into a soqlpoint, for exampole
 */
