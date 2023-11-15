package com.socrata.store.sqlizer

import com.rojoma.json.v3.ast.JString

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


object SoQLFunctionSqlizerTestRedshift {
  final abstract class TestMT extends MetaTypes with com.socrata.common.sqlizer.metatypes.SoQLMetaTypesExt {
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

  val TestFuncallSqlizer = new com.socrata.common.sqlizer.SoQLFunctionSqlizerRedshift[TestMT]

  val TestSqlizer = new Sqlizer[TestMT](
    TestFuncallSqlizer,
    new com.socrata.common.sqlizer.RedshiftExprSqlFactory[TestMT],
    TestNamespaces,
    new com.socrata.common.sqlizer.SoQLRewriteSearch[TestMT](searchBeforeQuery = true),
    ProvenanceMapper,
    _ => false,
    (sqlizer, physicalTableFor, extraContext) =>
      new com.socrata.common.sqlizer.SoQLRepProviderRedshift[TestMT](
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
}

class SoQLFunctionSqlizerTestRedshift  {

  type TestMT = SoQLFunctionSqlizerTestRedshift.TestMT

  val sqlizer = SoQLFunctionSqlizerTestRedshift.TestSqlizer
  val funcallSqlizer = SoQLFunctionSqlizerTestRedshift.TestFuncallSqlizer

  def extraContext = new com.socrata.common.sqlizer.SoQLExtraContext(
    Map.empty,
    _ => Some(obfuscation.CryptProvider.zeros),
    Map.empty,
    s => s"'$s'"
  )

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[TestMT](items.toMap)
  val analyzer = new SoQLAnalyzer[TestMT](new SoQLTypeInfo2, SoQLFunctionInfo, SoQLFunctionSqlizerTestRedshift.ProvenanceMapper)
  def analyze(soqlexpr: String): String = {
    val s = analyzeStatement(s"SELECT ($soqlexpr)")
    val prefix = "SELECT "
    val suffix = " AS i1 FROM table1 AS x1"
    if(s.startsWith(prefix) && s.endsWith(suffix)) {
      s.dropRight(suffix.length).drop(prefix.length)
    } else {
      s
    }
  }
  def analyzeStatement(stmt: String, useSelectListReferences: Boolean = false): String = {
    val tf = MockTableFinder[TestMT](
      (0, "table1") -> D(
        "text" -> SoQLText,
        "num" -> SoQLNumber,
        "url" -> SoQLUrl,
        "geom" -> SoQLPolygon,
        "geometry_point" -> SoQLPoint
      )
    )

    val ft =
      tf.findTables(0, ResourceName("table1"), stmt, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => fail("Bad query: " + err)
      }

    var analysis =
      analyzer(ft, UserParameters.empty) match {
        case Right(an) => an
        case Left(err) => fail("Bad query: " + err)
      }

    if(useSelectListReferences) analysis = analysis.useSelectListReferences

    val sql = sqlizer(analysis, extraContext).getOrElse { fail("analysis failed") }.sql.layoutSingleLine.toString

    println(sql)
    sql
  }

  @Test
  @DisplayName("is null works")
  def is_null_works(): Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text is null"), """SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NULL""")
  }

  @Test
  @DisplayName("is not null works")
  def is_not_null_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text is not null"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NOT NULL"""))
  }

  @Test
  @DisplayName("not works")
  def not_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE NOT text = 'two'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE NOT((x1.text) = (text 'two'))"""))
  }

  @Test
  @DisplayName("between x and y works")
  def between_x_and_y_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num between 0 and 3"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("not between x and y works")
  def not_between_x_and_y_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num not between 0 and 3"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) not between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("in subset works")
  def in_subset_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num in (1, 2, 3)"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("in subset works case insensitively")
  def in_subset_works_case_insensitively: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE caseless_one_of(text, 'one', 'two', 'three')"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))"""))
  }

  @Test
  @DisplayName("not in works")
  def not_in_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num not in (1, 2, 3)"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) NOT IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("caseless not one of works")
  def caseless_not_one_of_works: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE caseless_not_one_of(text, 'one', 'two', 'three')"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) NOT IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))"""))
  }

  @Test
  @DisplayName("equal = works with int")
  def `equal_=_works_with_int`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num = 1"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("equal = works with text")
  def `equal = works with text`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text = 'TWO'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')"""))
  }

  @Test
  @DisplayName("equal == works with text")
  def `equal == works with text`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text == 'TWO'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')"""))
  }

  @Test
  @DisplayName("equal == works with int")
  def `equal == works with int`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num == 1"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("caseless equal works")
  def `caseless equal works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE caseless_eq(text, 'TWO')"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (upper(text 'TWO'))"""))
  }

  @Test
  @DisplayName("not equal <> works")
  def `not equal <> works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num <> 2"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("not equal != works")
  def `not equal != works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num != 2"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("caseless not equal works")
  def `caseless not equal works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE caseless_ne(text, 'TWO')"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) <> (upper(text 'TWO'))"""))
  }

  @Test
  @DisplayName("and works")
  def `and works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num == 1 and text == 'one'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) = (1 :: decimal(30, 7))) AND ((x1.text) = (text 'one'))"""))
  }

  @Test
  @DisplayName("or works")
  def `or works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num < 5 or text == 'two'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) < (5 :: decimal(30, 7))) OR ((x1.text) = (text 'two'))"""))
  }

  @Test
  @DisplayName("less than works")
  def `less than works`: Unit = {
      assertEquals(analyzeStatement("SELECT text, num WHERE num < 2.0001"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) < (2.0001 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("les than or equals works")
  def `les than or equals works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num <= 2.1"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <= (2.1 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("greater than works")
  def `greater than works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num > 0.9"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) > (0.9 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("greater than or equals works")
  def `greater than or equals works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE num >= 2"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) >= (2 :: decimal(30, 7))"""))
  }

  @Test
  @DisplayName("least works")
  def `least works`: Unit = {
    assertEquals(analyzeStatement("SELECT LEAST(1, 1.1, 0.9)"), ("""SELECT least(1 :: decimal(30, 7), 1.1 :: decimal(30, 7), 0.9 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("greatest works")
  def `greatest works`: Unit = {
    assertEquals(analyzeStatement("SELECT GREATEST(0.9, 1, 1.1)"), ("""SELECT greatest(0.9 :: decimal(30, 7), 1 :: decimal(30, 7), 1.1 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("like works with percent")
  def `like works with percent`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text LIKE 't%'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't%')"""))
  }

  @Test
  @DisplayName("like works with underscore")
  def `like works with underscore`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text LIKE 't__'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't__')"""))
  }

  @Test
  @DisplayName("not like works with percent")
  def `not like works with percent`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text NOT LIKE 't%'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't%')"""))
  }

  @Test
  @DisplayName("not like works with underscore")
  def `not like works with underscore`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num WHERE text NOT LIKE 't__'"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't__')"""))
  }

  @Test
  @DisplayName("concat || works")
  def `concat || works`: Unit = {
    assertEquals(analyzeStatement("SELECT text || num"), ("""SELECT (x1.text) || (x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("lower() works")
  def `lower() works`: Unit = {
    assertEquals(analyzeStatement("SELECT lower(text), num where lower(text) == 'two'"), ("""SELECT lower(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (lower(x1.text)) = (text 'two')"""))
  }

  @Test
  @DisplayName("upper() works")
  def `upper() works`: Unit = {
    assertEquals(analyzeStatement("SELECT upper(text), num where upper(text) == 'TWO'"), ("""SELECT upper(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (text 'TWO')"""))
  }

  @Test
  @DisplayName("length() works")
  def `length() works`: Unit = {
    assertEquals(analyzeStatement("SELECT length(text), num"), ("""SELECT length(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("replace() works")
  def `replace() works`: Unit = {
    assertEquals(analyzeStatement("SELECT num ,replace(text, 'o', 'z')"), ("""SELECT x1.num AS i1, replace(x1.text, text 'o', text 'z') AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("trimming on both ends works")
  def `trimming on both ends works`: Unit = {
    assertEquals(analyzeStatement("SELECT trim('   abc   ')"), ("""SELECT trim(text '   abc   ') AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("trimming on leading spaces works")
  def `trimming on leading spaces works`: Unit = {
    assertEquals(analyzeStatement("SELECT trim_leading('   abc   ')"), ("""SELECT ltrim(text '   abc   ') AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("trimming on trailing spaces works")
  def `trimming on trailing spaces works`: Unit = {
    assertEquals(analyzeStatement("SELECT trim_trailing('   abc   ')"), ("""SELECT rtrim(text '   abc   ') AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("left_pad works")
  def `left_pad works`: Unit = {
    assertEquals(analyzeStatement("SELECT left_pad(text, 10, 'a'), num"), ("SELECT lpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("right_pad works")
  def `right_pad works`: Unit = {
    assertEquals(analyzeStatement("SELECT right_pad(text, 10, 'a'), num"), ("SELECT rpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("chr() works")
  def `chr() works`: Unit = {
    assertEquals(analyzeStatement("SELECT chr(50.2)"), ("SELECT chr((50.2 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("substring(characters, start_index base 1) works")
  def `substring(characters, start_index base 1) works`: Unit = {
    assertEquals(analyzeStatement("SELECT substring('abcdefghijk', 3)"), ("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("substring(characters, start_index base 1, length) works")
  def `substring(characters, start_index base 1, length) works`: Unit = {
    assertEquals(analyzeStatement("SELECT substring('abcdefghijk', 3, 4)"), ("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int, (4 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("split_part works")
  def `split_part works`: Unit = {
    assertEquals(analyzeStatement("SELECT split_part(text, '.', 3)"), ("SELECT split_part(x1.text, text '.', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1"))
  }

  @Test
  @DisplayName("uniary minus works")
  def `uniary minus works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, - num"), ("""SELECT x1.text AS i1, -(x1.num) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("uniary plus works")
  def `uniary plus works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, + num"), ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("binary minus works")
  def `binary minus works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num - 1"), ("""SELECT x1.text AS i1, (x1.num) - (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("binary plus works")
  def `binary plus works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num + 1"), ("""SELECT x1.text AS i1, (x1.num) + (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("num times num works")
  def `num times num works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num * 2"), ("""SELECT x1.text AS i1, (x1.num) * (2 :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("doube times double works")
  def `doube times double works`: Unit = {
    assertEquals(analyzeStatement("SELECT 5.4567 * 9.94837"), ("""SELECT (5.4567 :: decimal(30, 7)) * (9.94837 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("division works")
  def `division works`: Unit = {
    assertEquals(analyzeStatement("SELECT 6.4354 / 3.423"), ("""SELECT (6.4354 :: decimal(30, 7)) / (3.423 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("exponents work")
  def `exponents work`: Unit = {
    assertEquals(analyzeStatement("SELECT 7.4234 ^ 2"), ("""SELECT (7.4234 :: decimal(30, 7)) ^ (2 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("modulo works")
  def `modulo works`: Unit = {
    assertEquals(analyzeStatement("SELECT 6.435 % 3.432"), ("""SELECT (6.435 :: decimal(30, 7)) % (3.432 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("ln works")
  def `ln works`: Unit = {
    assertEquals(analyzeStatement("SELECT ln(16)"), ("""SELECT ln(16 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("absolute works")
  def `absolute works`: Unit = {
    assertEquals(analyzeStatement("SELECT abs(-1.234)"), ("""SELECT abs(-1.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("ceil() works")
  def `ceil() works`: Unit = {
    assertEquals(analyzeStatement("SELECT ceil(4.234)"), ("""SELECT ceil(4.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("floor works")
  def `floor works`: Unit = {
    assertEquals(analyzeStatement("SELECT floor(9.89)"), ("""SELECT floor(9.89 :: decimal(30, 7)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("contains works")
  def `contains works`: Unit = {
    assertEquals(analyzeStatement("SELECT text where contains(text, 'a')"), ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(text 'a' in x1.text) <> 0)"""))
  }

  @Test
  @DisplayName("caseless contains works")
  def `caseless contains works`: Unit = {
    assertEquals(analyzeStatement("SELECT text where caseless_contains(text, 'o')"), ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(upper(text 'o') in upper(x1.text)) <> 0)"""))
  }

  @Test
  @DisplayName("starts_with works")
  def `starts_with works`: Unit = {
    assertEquals(analyzeStatement("SELECT text where starts_with(text, 'o')"), ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ text 'o' = left(x1.text, length(text 'o')))"""))
  }

  @Test
  @DisplayName("caseless starts_with works")
  def `caseless starts_with works`: Unit = {
    assertEquals(analyzeStatement("SELECT text where caseless_starts_with(text, 'o')"), ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ upper(text 'o') = left(upper(x1.text), length(upper(text 'o'))))"""))
  }

  @Test
  @DisplayName("round works")
  def `round works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, round(num, 2)"), ("""SELECT x1.text AS i1, (/* soql_round */ round(x1.num, 2 :: decimal(30, 7) :: int) :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("ToFloatingTimestamp")
  def `ToFloatingTimestamp`: Unit = {
    assertEquals(analyze("""to_floating_timestamp("2022-12-31T23:59:59Z", "America/New_York")"""), """(timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York')""")
  }

  @Test
  @DisplayName("FloatingTimeStampTruncYmd")
  def `FloatingTimeStampTruncYmd`: Unit = {
    assertEquals(analyze("date_trunc_ymd('2022-12-31T23:59:59')"), ("""date_trunc('day', timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampTruncYm")
  def `FloatingTimeStampTruncYm`: Unit = {
    assertEquals(analyze("date_trunc_ym('2022-12-31T23:59:59')"), ("""date_trunc('month', timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampTruncY")
  def `FloatingTimeStampTruncY`: Unit = {
    assertEquals(analyze("date_trunc_y('2022-12-31T23:59:59')"), ("""date_trunc('year', timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FixedTimeStampZTruncYmd")
  def `FixedTimeStampZTruncYmd`: Unit = {
    assertEquals(analyze("datez_trunc_ymd('2022-12-31T23:59:59Z')"), ("""date_trunc('day', timestamp with time zone '2022-12-31T23:59:59.000Z')"""))
  }

  @Test
  @DisplayName("FixedTimeStampZTruncYm")
  def `FixedTimeStampZTruncYm`: Unit = {
    assertEquals(analyze("datez_trunc_ym('2022-12-31T23:59:59Z')"), ("""date_trunc('month', timestamp with time zone '2022-12-31T23:59:59.000Z')"""))
  }

  @Test
  @DisplayName("FixedTimeStampZTruncY")
  def `FixedTimeStampZTruncY`: Unit = {
    assertEquals(analyze("datez_trunc_y('2022-12-31T23:59:59Z')"), ("""date_trunc('year', timestamp with time zone '2022-12-31T23:59:59.000Z')"""))
  }

  @Test
  @DisplayName("FixedTimeStampTruncYmdAtTimeZone")
  def `FixedTimeStampTruncYmdAtTimeZone`: Unit = {
    assertEquals(analyze("date_trunc_ymd('2022-12-31T23:59:59Z', 'America/New_York')"), ("""date_trunc('day', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""))
  }

  @Test
  @DisplayName("FixedTimeStampTruncYmAtTimeZone")
  def `FixedTimeStampTruncYmAtTimeZone`: Unit = {
    assertEquals(analyze("date_trunc_ym('2022-12-31T23:59:59Z', 'America/New_York')"), ("""date_trunc('month', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""))
  }

  @Test
  @DisplayName("FixedTimeStampTruncYAtTimeZone")
  def `FixedTimeStampTruncYAtTimeZone`: Unit = {
    assertEquals(analyze("date_trunc_y('2022-12-31T23:59:59Z', 'America/New_York')"), ("""date_trunc('year', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractY")
  def `FloatingTimeStampExtractY`: Unit = {
    assertEquals(analyze("date_extract_y('2022-12-31T23:59:59')"), ("""extract(year from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractM")
  def `FloatingTimeStampExtractM`: Unit = {
    assertEquals(analyze("date_extract_m('2022-12-31T23:59:59')"), ("""extract(month from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractD")
  def `FloatingTimeStampExtractD`: Unit = {
    assertEquals(analyze("date_extract_d('2022-12-31T23:59:59')"), ("""extract(day from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractHh")
  def `FloatingTimeStampExtractHh`: Unit = {
    assertEquals(analyze("date_extract_hh('2022-12-31T23:59:59')"), ("""extract(hour from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractMm")
  def `FloatingTimeStampExtractMm`: Unit = {
    assertEquals(analyze("date_extract_mm('2022-12-31T23:59:59')"), ("""extract(minute from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractSs")
  def `FloatingTimeStampExtractSs`: Unit = {
    assertEquals(analyze("date_extract_ss('2022-12-31T23:59:59')"), ("""extract(second from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractDow")
  def `FloatingTimeStampExtractDow`: Unit = {
    assertEquals(analyze("date_extract_dow('2022-12-31T23:59:59')"), ("""extract(dayofweek from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimeStampExtractWoy")
  def `FloatingTimeStampExtractWoy`: Unit = {
    assertEquals(analyze("date_extract_woy('2022-12-31T23:59:59')"), ("""extract(week from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("FloatingTimestampExtractIsoY")
  def `FloatingTimestampExtractIsoY`: Unit = {
    assertEquals(analyze("date_extract_iso_y('2022-12-31T23:59:59')"), ("""extract(year from timestamp without time zone '2022-12-31T23:59:59.000')"""))
  }

  @Test
  @DisplayName("EpochSeconds")
  def `EpochSeconds`: Unit = {
    assertEquals(analyze("epoch_seconds('2022-12-31T23:59:59Z')"), ("""extract(epoch from timestamp with time zone '2022-12-31T23:59:59.000Z')"""))
  }

  @Test
  @DisplayName("TimeStampDiffD")
  def `TimeStampDiffD`: Unit = {
    assertEquals(analyze("date_diff_d('2022-12-31T23:59:59Z', '2022-01-01T00:00:00Z')"), ("""datediff(day, timestamp with time zone '2022-12-31T23:59:59.000Z' at time zone (text 'UTC'), timestamp with time zone '2022-01-01T00:00:00.000Z' at time zone (text 'UTC'))"""))
  }

  @Test
  @DisplayName("SignedMagnitude10")
  def `SignedMagnitude10`: Unit = {
    assertEquals(analyzeStatement("select signed_magnitude_10(num)"), ("""SELECT ((/* soql_signed_magnitude_10 */ (sign(x1.num) * length(floor(abs(x1.num)) :: text)))) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("SignedMagnitudeLinear")
  def `SignedMagnitudeLinear`: Unit = {
    assertEquals(analyzeStatement("select signed_magnitude_Linear(num, 8)"), ("""SELECT ((/* soql_signed_magnitude_linear */ (case when (8 :: decimal(30, 7)) = 1 then floor(x1.num) else sign(x1.num) * floor(abs(x1.num)/(8 :: decimal(30, 7)) + 1) end))) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("TimeStampAdd")
  def `TimeStampAdd`: Unit = {
    assertEquals(analyze("date_add('2022-12-31T23:59:59Z', 'P1DT1H')"), ("""(timestamp with time zone '2022-12-31T23:59:59.000Z') + (interval '1 days, 1 hours')"""))
  }

  @Test
  @DisplayName("TimeStampPlus")
  def `TimeStampPlus`: Unit = {
    assertEquals(analyze("('2022-12-31T23:59:59Z' + 'P1001Y1DT1H1S')"), ("""(timestamp with time zone '2022-12-31T23:59:59.000Z') + (interval '1 millenniums, 1 years, 1 days, 1 hours, 1 seconds')"""))
  }

  @Test
  @DisplayName("TimeStampMinus")
  def `TimeStampMinus`: Unit = {
    assertEquals(analyze("('2022-12-31T23:59:59Z' - 'P1001Y1DT1H1S')"), ("""(timestamp with time zone '2022-12-31T23:59:59.000Z') - (interval '1 millenniums, 1 years, 1 days, 1 hours, 1 seconds')"""))
  }

  @Test
  @DisplayName("GetUtcDate")
  def `GetUtcDate`: Unit = {
    assertEquals(analyze("get_utc_date()"), ("""current_date at time zone 'UTC'"""))
  }

//  tests for aggregate functions
  @Test
  @DisplayName("max works")
  def `max works`: Unit = {
    assertEquals(analyzeStatement("SELECT max(num)"), ("""SELECT max(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("min works")
  def `min works`: Unit = {
    assertEquals(analyzeStatement("SELECT min(num)"), ("""SELECT min(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("count(*) works")
  def `count(*) works`: Unit = {
    assertEquals(analyzeStatement("SELECT count(*)"), ("""SELECT (count(*)) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("count() works")
  def `count() works`: Unit = {
    assertEquals(analyzeStatement("SELECT count(text)"), ("""SELECT (count(x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("count(iif ... ) works")
  def `count(iif ... ) works`: Unit = {
    assertEquals(analyzeStatement("SELECT count(IIF(text='one', 1, NULL))"), ("""SELECT (count(CASE WHEN (x1.text) = (text 'one') THEN 1 :: decimal(30, 7) ELSE null :: decimal(30, 7) END)) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("count_distinct works")
  def `count_distinct works`: Unit = {
    assertEquals(analyzeStatement("SELECT count_distinct(text)"), ("""SELECT (count(DISTINCT x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("sum works")
  def `sum works`: Unit = {
    assertEquals(analyzeStatement("SELECT sum(num)"), ("""SELECT sum(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("avg works")
  def `avg works`: Unit = {
    assertEquals(analyzeStatement("SELECT avg(num)"), ("""SELECT avg(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("median works")
  def `median works`: Unit = {
    assertEquals(analyzeStatement("SELECT median(num)"), ("""SELECT median(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("stddev_pop works")
  def `stddev_pop works`: Unit = {
    assertEquals(analyzeStatement("SELECT stddev_pop(num)"), ("""SELECT stddev_pop(x1.num) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("stddev_samp")
  def `stddev_samp`: Unit = {
    assertEquals(analyzeStatement("SELECT stddev_samp(num)"), ("""SELECT stddev_samp(x1.num) AS i1 FROM table1 AS x1"""))
  }

//  tests for conditional functions
  @Test
  @DisplayName("nullif works")
  def `nullif works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, nullif(num, 1)"), ("""SELECT x1.text AS i1, nullif(x1.num, 1 :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("coalesce works")
  def `coalesce works`: Unit = {
    assertEquals(analyzeStatement("SELECT coalesce(text, 'zero'), coalesce(num, '0')"), ("""SELECT coalesce(x1.text, text 'zero') AS i1, coalesce(x1.num, 0 :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("case works")
  def `case works`: Unit = {
    assertEquals(analyzeStatement("SELECT num, case(num > 1, 'large num', num <= 1, 'small num')"), ("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' WHEN (x1.num) <= (1 :: decimal(30, 7)) THEN text 'small num' END AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("iif works")
  def `iif works`: Unit = {
    assertEquals(analyzeStatement("SELECT num, iif(num > 1, 'large num', 'small num')"), ("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' ELSE text 'small num' END AS i2 FROM table1 AS x1"""))
  }

//  tests for geo functions
  @Test
  @DisplayName("spacial union works for polygons and points")
  def `spacial union works for polygons and points`: Unit = {
    assertEquals(analyzeStatement("Select spatial_union(geom, geom)"), ("""SELECT st_asbinary(st_multi(st_union(x1.geom, x1.geom))) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("from line to multiline, from polygon to multipolygon and from line to multiline work")
  def `from line to multiline, from polygon to multipolygon and from line to multiline work`: Unit = {
    assertEquals(analyzeStatement("SELECT geo_multi(geom)"), ("""SELECT st_asbinary(st_multi(x1.geom)) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("num_points works")
  def `num_points works`: Unit = {
    assertEquals(analyzeStatement("SELECT num_points(geom)"), ("""SELECT (st_npoints(x1.geom)) :: decimal(30, 7) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("crosses works")
  def `crosses works`: Unit = {
    assertEquals(analyzeStatement("SELECT crosses(geom, geom)"), ("""SELECT st_crosses(x1.geom, x1.geom) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("intersection works")
  def `intersection works`: Unit = {
    assertEquals(analyzeStatement("SELECT polygon_intersection(geom, geom)"), ("""SELECT st_asbinary(st_multi(st_buffer(st_intersection(x1.geom, x1.geom), 0.0))) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("intersects works")
  def `intersects works`: Unit = {
    assertEquals(analyzeStatement("SELECT intersects(geom, geom)"), ("""SELECT st_intersects(x1.geom, x1.geom) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("within_polygon works")
  def `within_polygon works`: Unit = {
    assertEquals(analyzeStatement("SELECT within_polygon(geom, geom)"), ("""SELECT st_within(x1.geom, x1.geom) AS i1 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("within_box works")
  def `within_box works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, within_box(geom, 23, 34, 10, 56)"), ("""SELECT x1.text AS i1, (/* within_box */ st_contains(st_makeenvelope(34 :: decimal(30, 7) :: DOUBLE PRECISION, 10 :: decimal(30, 7) :: DOUBLE PRECISION, 56 :: decimal(30, 7) :: DOUBLE PRECISION, 23 :: decimal(30, 7) :: DOUBLE PRECISION), x1.geom)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("is_empty works")
  def `is_empty works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, is_empty(geom)"), ("""SELECT x1.text AS i1, st_isempty(x1.geom) or (x1.geom) is null AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("simplify works")
  def `simplify works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, simplify(geom, 1)"), ("""SELECT x1.text AS i1, st_asbinary(st_simplify(x1.geom, 1 :: decimal(30, 7))) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("area works")
  def `area works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, area(geom)"), ("""SELECT x1.text AS i1, (/* soql_area */ st_area(x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("distance_in_meters works for points only")
  def `distance_in_meters works for points only`: Unit = {
    assertEquals(analyzeStatement("SELECT text, distance_in_meters(geom, geom)"), ("""SELECT x1.text AS i1, (/* soql_distance_in_meters */ st_distance(x1.geom :: geography, x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("visible_at works")
  def `visible_at works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, visible_at(geom, 3)"), ("""SELECT x1.text AS i1, (/* soql_visible_at */ (not st_isempty(x1.geom)) AND (st_geometrytype(x1.geom) = 'ST_Point' OR st_geometrytype(x1.geom) = 'ST_MultiPoint' OR (ST_XMax(x1.geom) - ST_XMin(x1.geom)) >= 3 :: decimal(30, 7) OR (ST_YMax(x1.geom) - ST_YMin(x1.geom)) >= 3 :: decimal(30, 7))) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("convex_hull works")
  def `convex_hull works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, convex_hull(geom)"), ("""SELECT x1.text AS i1, st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0))) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("curated_region_test works")
  def `curated_region_test works`: Unit = {
    assertEquals(analyzeStatement("Select text, curated_region_test(geom, 5)"), ("""SELECT x1.text AS i1, (/* soql_curated_region_test */ case when st_npoints(x1.geom) > 5 :: decimal(30, 7) then 'too complex' when st_xmin(x1.geom) < -180 or st_xmax(x1.geom) > 180 or st_ymin(x1.geom) < -90 or st_ymax(x1.geom) > 90 then 'out of bounds' when not st_isvalid(x1.geom) then 'invalid geography data' when x1.geom is null then 'empty' end) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("test for PointToLatitude")
  def `test for PointToLatitude`: Unit = {
    assertEquals(analyzeStatement("SELECT text, point_latitude(geometry_point)"), ("""SELECT x1.text AS i1, (st_y(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("test for PointToLongitude")
  def `test for PointToLongitude`: Unit = {
    assertEquals(analyzeStatement("SELECT text, point_longitude(geometry_point)"), ("""SELECT x1.text AS i1, (st_x(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1"""))
  }

//  tests for window functions
  @Test
  @DisplayName("row_number works")
  def `row_number works`: Unit = {
    assertEquals(analyzeStatement("SELECT text,num, row_number() over(partition by text order by num)"), ("""SELECT x1.text AS i1, x1.num AS i2, row_number() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("rank works")
  def `rank works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, rank() over(partition by text order by num)"), ("""SELECT x1.text AS i1, rank() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i2 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("dense_rank works")
  def `dense_rank works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, dense_rank() over(partition by text order by num)"), ("""SELECT x1.text AS i1, x1.num AS i2, dense_rank() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("first_value works")
  def `first_value works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, first_value(num) over(partition by text order by num rows between unbounded preceding and current row)"), ("""SELECT x1.text AS i1, x1.num AS i2, first_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("last_value works")
  def `last_value works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, last_value(num) over(partition by text order by num rows between unbounded preceding and current row)"), ("""SELECT x1.text AS i1, x1.num AS i2, last_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("lead works")
  def `lead works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, lead(num) over(partition by text order by num)"), ("""SELECT x1.text AS i1, x1.num AS i2, lead(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("leadOffset works")
  def `leadOffset works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, lead(num, 2) over(partition by text order by num)"), ("""SELECT x1.text AS i1, x1.num AS i2, lead(x1.num, (2 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("lag works")
  def `lag works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, lag(num) over(partition by text order by num desc)"), ("""SELECT x1.text AS i1, x1.num AS i2, lag(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num DESC NULLS FIRST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("lagOffset works")
  def `lagOffset works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, lag(num, 2) over(partition by text order by num desc)"), ("""SELECT x1.text AS i1, x1.num AS i2, lag(x1.num, (2 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num DESC NULLS FIRST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("ntile works")
  def `ntile works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, ntile(4) over(partition by text order by num)"), ("""SELECT x1.text AS i1, x1.num AS i2, ntile((4 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) max works")
  def `(window function) max works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, max(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, max(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) min works")
  def `(window function) min works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, min(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, min(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) count(*) works")
  def `(window function) count(*) works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, count(*) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, (count(*) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) :: decimal(30, 7) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) count() works")
  def `(window function) count() works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, count(text) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, (count(x1.text) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) :: decimal(30, 7) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) sum works")
  def `(window function) sum works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, sum(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, sum(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS y1"""))
  }

  @Test
  @DisplayName("(window function) avg works")
  def `(window function) avg works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, avg(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, avg(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) median works")
  def `(window function) median works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, median(num) over(partition by text)"), ("""SELECT x1.text AS i1, x1.num AS i2, median(x1.num) OVER (PARTITION BY x1.text) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) stddev_pop works")
  def `(window function) stddev_pop works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, stddev_pop(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, stddev_pop(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1"""))
  }

  @Test
  @DisplayName("(window function) stddev_samp works")
  def `(window function) stddev_samp works`: Unit = {
    assertEquals(analyzeStatement("SELECT text, num, stddev_samp(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"), ("""SELECT x1.text AS i1, x1.num AS i2, stddev_samp(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1"""))
  }
}
