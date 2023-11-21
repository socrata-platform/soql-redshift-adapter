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

object Utils {

  def printTable[T](conn: java.sql.Connection, tableName: String, transform: Option[String])(fn: java.sql.ResultSet => T) = {
    val col = transform.fold("testcol")(transform => s"""${transform}(testcol)""")
    val query = s"select $col from $tableName"
    Using.resource(conn.createStatement()) { stmt =>
      Using.resource((stmt.executeQuery(query))) { rs =>
        println(ResultSet.toList(rs)(fn))
      }
    }
  }

  def withTable(dataSource: AgroalDataSource, tableName: String)(columnName: String, columnType: String)(fn: (java.sql.Connection, String) => Unit) =
    Using.resource(dataSource.getConnection) { conn =>
      try {
        Using.resource(conn.createStatement()) { stmt =>
          stmt.executeUpdate(
            s"""create table $tableName ($columnName $columnType)""")
        }
        fn(conn, tableName)
      } finally {
        Using.resource(conn.createStatement()) { stmt =>
          stmt.executeUpdate(
            s"""drop table if exists "$tableName"""")
        }
      }
    }
}

object TableCreationTest {
  object ProvenanceMapper extends types.ProvenanceMapper[DatabaseNamesMetaTypes] {
    def toProvenance(dtn: types.DatabaseTableName[DatabaseNamesMetaTypes]): Provenance = {
      val DatabaseTableName(name) = dtn
      Provenance(name.name)
    }

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseNamesMetaTypes] = {
      val Provenance(name) = prov
      DatabaseTableName(AugmentedTableName(name, false))
    }
  }

  object TestNamespaces extends SqlNamespaces[DatabaseNamesMetaTypes] {
    override def rawDatabaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      name.name
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
    Map.empty,
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
class RepsLiterals {
  @DataSource("store")
  @Inject
  var dataSource: AgroalDataSource = _

  val repProvider = TableCreationTest.TestRepProvider
  val schema = SchemaImpl(repProvider)
  val rows = RowsImpl(repProvider)

  def testFails[T <: Throwable](literal: DatabaseNamesMetaTypes#ColumnValue)(expectedType: Class[T]) = {
    assertThrows(expectedType, () =>
      repProvider
        .reps(literal.typ).literal(LiteralValue[DatabaseNamesMetaTypes](literal)(AtomicPositionInfo.None))
    )
  }

  def test(testName: String)(literal: DatabaseNamesMetaTypes#ColumnValue)(expected: String*) = {
    val rep = repProvider
      .reps(literal.typ)

    println(s"start $testName")
    rep.literal(LiteralValue[DatabaseNamesMetaTypes](literal)(AtomicPositionInfo.None)).sqls.map(_.toString)
      .zipExact(expected.toList)
      .foreach { case (received, expected) => {
        assertEquals(expected, received)
        Utils.withTable(dataSource, "repsLiteral")("foo", "int") { (conn, tableName) =>
          schema.update(AugmentedTableName(tableName, false), "testcol")(literal.typ).foreach(thing => thing.execute(conn))
          rows.update(AugmentedTableName(tableName, false), "testcol")(literal).foreach(thing => thing.execute(conn))
          Utils.printTable(conn, tableName, Casts.casts.get(literal.typ))(rep.extractFrom(false)(_, 1))
        }
      }}
    println(s"end $testName")
    println()
    println()
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

  // TODO: probably broken -- think about this
  @Test
  def json(): Unit = {
    test("json")(SoQLJson(JNumber(2)))("JSON_PARSE(2)")
    test("json")(SoQLJson(JNumber(2.18)))("JSON_PARSE(2.18)")
    test("json")(SoQLJson(j"""{"foo": 22}"""))("""JSON_PARSE('{"foo":22}')""")
    test("json null")(SoQLJson(JNull))("JSON_PARSE(null)")
    test("json")(SoQLJson(JArray(Seq(JNumber(2), JString("foo")))))("""JSON_PARSE('[2,"foo"]')""")
  }

  @Test
  def document(): Unit = {
    testFails(SoQLDocument("", None, None))(classOf[NotImplementedError])
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
)""")
  }

  val lineString = new LineString(Array(coordinate, coordinate), precisionModel, Geo.defaultSRID)

  @Test
  def line(): Unit = {
    test("line")(SoQLLine(lineString))(
      """ST_GeomFromWKB(
  '0000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  @Test
  def multiline(): Unit = {
    test("multiline")(SoQLMultiLine(new MultiLineString(Array(lineString, lineString), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '0000000005000000020000000002000000024059000000000000408f3800000000004059000000000000408f3800000000000000000002000000024059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  val poly = new Polygon(new LinearRing(Array(coordinate, coordinate, coordinate, coordinate), precisionModel, Geo.defaultSRID), precisionModel, Geo.defaultSRID)

  @Test
  def polygon(): Unit = {
    test("polygon")(SoQLPolygon(poly))(
      """ST_GeomFromWKB(
  '000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
  }

  @Test
  def multipolygon(): Unit = {
    test("polygon")(SoQLMultiPolygon(new MultiPolygon(Array(poly, poly, poly), precisionModel, Geo.defaultSRID)))(
      """ST_GeomFromWKB(
  '000000000600000003000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000000000000300000001000000044059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f3800000000004059000000000000408f380000000000',
  4326
)""")
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
          schema.update(AugmentedTableName(tableName, false), "testcol")(`type`).foreach(_.execute(conn))
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
 test column create commands (compressedSubCols and stuff)
 test compression of bag of columns into a super

 make tests construct real tables and run queries against them
 make tests construct real literals and verify they work


make sure we can read these literals when written to a table. make sure we can read back into a soqlpoint, for exampole

do compound types
do ID and version


connect to redshift
test various soql commands that may fail due to super stuff

why is the rep stuff so tied to the Sqlizer?

 */
