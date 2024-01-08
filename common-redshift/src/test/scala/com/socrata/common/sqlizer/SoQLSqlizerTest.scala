package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

import com.socrata.soql.environment.Provenance

import io.quarkus.test.junit.QuarkusTest
import org.junit.jupiter.api.{Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._
import com.socrata.common.sqlizer.metatypes._

object SoQLSqlizerTest {

  object ProvenanceMapper
      extends types.ProvenanceMapper[DatabaseNamesMetaTypes] {
    def toProvenance(
        dtn: types.DatabaseTableName[DatabaseNamesMetaTypes]
    ): Provenance = {
      val DatabaseTableName(name) = dtn
      Provenance(name)
    }

    def fromProvenance(
        prov: Provenance
    ): types.DatabaseTableName[DatabaseNamesMetaTypes] = {
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
    protected override def idxPrefix: String = "idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
  }

  val TestFuncallSqlizer =
    new SoQLFunctionSqlizerRedshift[DatabaseNamesMetaTypes]

  val TestSqlizer = new Sqlizer[DatabaseNamesMetaTypes](
    TestFuncallSqlizer,
    new RedshiftExprSqlFactory[DatabaseNamesMetaTypes],
    TestNamespaces,
    new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true),
    ProvenanceMapper,
    _ => false,
    (sqlizer, _, extraContext) =>
      new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
        extraContext.cryptProviderProvider,
        sqlizer.namespace,
        sqlizer.exprSqlFactory,
        sqlizer.toProvenance
      ) {
        override def mkStringLiteral(s: String) = Doc(
          extraContext.escapeString(s)
        )
      }
  )
}

@QuarkusTest
class SoQLSqlizerTest {

  val sqlizer = SoQLSqlizerTest.TestSqlizer

  def extraContext = new SoQLExtraContext(
    Map.empty,
    _ => Some(obfuscation.CryptProvider.zeros),
    s => s"'$s'"
  )

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[DatabaseNamesMetaTypes](items.toMap)

  val analyzer =
    new SoQLAnalyzer[DatabaseNamesMetaTypes](
      new SoQLTypeInfo2,
      SoQLFunctionInfo,
      SoQLSqlizerTest.ProvenanceMapper
    )

  def analyzeStatement(stmt: String) = analyze(
    stmt
  ).sql.layoutSingleLine.toString

  def analyze(
      stmt: String
  ): com.socrata.soql.sqlizer.Sqlizer.Result[DatabaseNamesMetaTypes] = {
    val tf = MockTableFinder[DatabaseNamesMetaTypes](
      (0, "table1") -> D(
        "text" -> SoQLText,
        "num" -> SoQLNumber,
        "geom" -> SoQLPolygon,
        "geometry_point" -> SoQLPoint
      )
    )

    val ft =
      tf.findTables(0, ResourceName("table1"), stmt, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => fail("Bad query: " + err)
      }

    val analysis =
      analyzer(ft, UserParameters.empty) match {
        case Right(an) => an
        case Left(err) => fail("Bad query: " + err)
      }

    sqlizer.apply(analysis, extraContext).getOrElse { fail("analysis failed") }
  }

  def test(generated: String, expected: String) = {
    println(generated)
    assertEquals(generated, expected)
  }

  @Test
  def is_null_works(): Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text is null"),
      """SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NULL"""
    )
  }

  @Test
  def is_not_null_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text is not null"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NOT NULL""")
    )
  }

  @Test
  def not_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE NOT text = 'two'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE NOT((x1.text) = (text 'two'))""")
    )
  }

  @Test
  def between_x_and_y_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num between 0 and 3"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))""")
    )
  }

  @Test
  def not_between_x_and_y_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num not between 0 and 3"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) not between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))""")
    )
  }

  @Test
  def in_subset_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num in (1, 2, 3)"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))""")
    )
  }

  @Test
  def in_subset_works_case_insensitively: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num WHERE caseless_one_of(text, 'one', 'two', 'three')"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))""")
    )
  }

  @Test
  def not_in_works: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num not in (1, 2, 3)"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) NOT IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))""")
    )
  }

  @Test
  def caseless_not_one_of_works: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num WHERE caseless_not_one_of(text, 'one', 'two', 'three')"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) NOT IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))""")
    )
  }

  @Test
  def `equal_=_works_with_int`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num = 1"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))""")
    )
  }

  @Test
  def `equal = works with text`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text = 'TWO'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')""")
    )
  }

  @Test
  def `equal == works with text`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text == 'TWO'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')""")
    )
  }

  @Test
  def `equal == works with int`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num == 1"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))""")
    )
  }

  @Test
  def `caseless equal works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE caseless_eq(text, 'TWO')"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (upper(text 'TWO'))""")
    )
  }

  @Test
  def `not equal <> works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num <> 2"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))""")
    )
  }

  @Test
  def `not equal != works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num != 2"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))""")
    )
  }

  @Test
  def `caseless not equal works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE caseless_ne(text, 'TWO')"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) <> (upper(text 'TWO'))""")
    )
  }

  @Test
  def `and works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num == 1 and text == 'one'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) = (1 :: decimal(30, 7))) AND ((x1.text) = (text 'one'))""")
    )
  }

  @Test
  def `or works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num < 5 or text == 'two'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) < (5 :: decimal(30, 7))) OR ((x1.text) = (text 'two'))""")
    )
  }

  @Test
  def `less than works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num < 2.0001"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) < (2.0001 :: decimal(30, 7))""")
    )
  }

  @Test
  def `les than or equals works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num <= 2.1"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <= (2.1 :: decimal(30, 7))""")
    )
  }

  @Test
  def `greater than works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num > 0.9"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) > (0.9 :: decimal(30, 7))""")
    )
  }

  @Test
  def `greater than or equals works`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE num >= 2"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) >= (2 :: decimal(30, 7))""")
    )
  }

  @Test
  def `least works`: Unit = {
    test(
      analyzeStatement("SELECT LEAST(1, 1.1, 0.9)"),
      ("""SELECT least(1 :: decimal(30, 7), 1.1 :: decimal(30, 7), 0.9 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `greatest works`: Unit = {
    test(
      analyzeStatement("SELECT GREATEST(0.9, 1, 1.1)"),
      ("""SELECT greatest(0.9 :: decimal(30, 7), 1 :: decimal(30, 7), 1.1 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `like works with percent`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text LIKE 't%'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't%')""")
    )
  }

  @Test
  def `like works with underscore`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text LIKE 't__'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't__')""")
    )
  }

  @Test
  def `not like works with percent`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text NOT LIKE 't%'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't%')""")
    )
  }

  @Test
  def `not like works with underscore`: Unit = {
    test(
      analyzeStatement("SELECT text, num WHERE text NOT LIKE 't__'"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't__')""")
    )
  }

  @Test
  def `concat || works`: Unit = {
    test(
      analyzeStatement("SELECT text || num"),
      ("""SELECT (x1.text) || (x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `lower() works`: Unit = {
    test(
      analyzeStatement("SELECT lower(text), num where lower(text) == 'two'"),
      ("""SELECT lower(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (lower(x1.text)) = (text 'two')""")
    )
  }

  @Test
  def `upper() works`: Unit = {
    test(
      analyzeStatement("SELECT upper(text), num where upper(text) == 'TWO'"),
      ("""SELECT upper(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (text 'TWO')""")
    )
  }

  @Test
  def `length() works`: Unit = {
    test(
      analyzeStatement("SELECT length(text), num"),
      ("""SELECT length(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `replace() works`: Unit = {
    test(
      analyzeStatement("SELECT num ,replace(text, 'o', 'z')"),
      ("""SELECT x1.num AS i1, replace(x1.text, text 'o', text 'z') AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `trimming on both ends works`: Unit = {
    test(
      analyzeStatement("SELECT trim('   abc   ')"),
      ("""SELECT trim(text '   abc   ') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `trimming on leading spaces works`: Unit = {
    test(
      analyzeStatement("SELECT trim_leading('   abc   ')"),
      ("""SELECT ltrim(text '   abc   ') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `trimming on trailing spaces works`: Unit = {
    test(
      analyzeStatement("SELECT trim_trailing('   abc   ')"),
      ("""SELECT rtrim(text '   abc   ') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `left_pad works`: Unit = {
    test(
      analyzeStatement("SELECT left_pad(text, 10, 'a'), num"),
      ("SELECT lpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1")
    )
  }

  @Test
  def `right_pad works`: Unit = {
    test(
      analyzeStatement("SELECT right_pad(text, 10, 'a'), num"),
      ("SELECT rpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1")
    )
  }

  @Test
  def `chr() works`: Unit = {
    test(
      analyzeStatement("SELECT chr(50.2)"),
      ("SELECT chr((50.2 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
    )
  }

  @Test
  def `substring(characters, start_index base 1) works`: Unit = {
    test(
      analyzeStatement("SELECT substring('abcdefghijk', 3)"),
      ("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
    )
  }

  @Test
  def `substring(characters, start_index base 1, length) works`: Unit = {
    test(
      analyzeStatement("SELECT substring('abcdefghijk', 3, 4)"),
      ("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int, (4 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
    )
  }

  @Test
  def `split_part works`: Unit = {
    test(
      analyzeStatement("SELECT split_part(text, '.', 3)"),
      ("SELECT split_part(x1.text, text '.', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
    )
  }

  @Test
  def `uniary minus works`: Unit = {
    test(
      analyzeStatement("SELECT text, - num"),
      ("""SELECT x1.text AS i1, -(x1.num) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `uniary plus works`: Unit = {
    test(
      analyzeStatement("SELECT text, + num"),
      ("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `binary minus works`: Unit = {
    test(
      analyzeStatement("SELECT text, num - 1"),
      ("""SELECT x1.text AS i1, (x1.num) - (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `binary plus works`: Unit = {
    test(
      analyzeStatement("SELECT text, num + 1"),
      ("""SELECT x1.text AS i1, (x1.num) + (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `num times num works`: Unit = {
    test(
      analyzeStatement("SELECT text, num * 2"),
      ("""SELECT x1.text AS i1, (x1.num) * (2 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `doube times double works`: Unit = {
    test(
      analyzeStatement("SELECT 5.4567 * 9.94837"),
      ("""SELECT (5.4567 :: decimal(30, 7)) * (9.94837 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `division works`: Unit = {
    test(
      analyzeStatement("SELECT 6.4354 / 3.423"),
      ("""SELECT (6.4354 :: decimal(30, 7)) / (3.423 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `exponents work`: Unit = {
    test(
      analyzeStatement("SELECT 7.4234 ^ 2"),
      ("""SELECT (7.4234 :: decimal(30, 7)) ^ (2 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `modulo works`: Unit = {
    test(
      analyzeStatement("SELECT 6.435 % 3.432"),
      ("""SELECT (6.435 :: decimal(30, 7)) % (3.432 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `ln works`: Unit = {
    test(
      analyzeStatement("SELECT ln(16)"),
      ("""SELECT ln(16 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `absolute works`: Unit = {
    test(
      analyzeStatement("SELECT abs(-1.234)"),
      ("""SELECT abs(-1.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `ceil() works`: Unit = {
    test(
      analyzeStatement("SELECT ceil(4.234)"),
      ("""SELECT ceil(4.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `floor works`: Unit = {
    test(
      analyzeStatement("SELECT floor(9.89)"),
      ("""SELECT floor(9.89 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `contains works`: Unit = {
    test(
      analyzeStatement("SELECT text where contains(text, 'a')"),
      ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(text 'a' in x1.text) <> 0)""")
    )
  }

  @Test
  def `caseless contains works`: Unit = {
    test(
      analyzeStatement("SELECT text where caseless_contains(text, 'o')"),
      ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(upper(text 'o') in upper(x1.text)) <> 0)""")
    )
  }

  @Test
  def `starts_with works`: Unit = {
    test(
      analyzeStatement("SELECT text where starts_with(text, 'o')"),
      ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ text 'o' = left(x1.text, length(text 'o')))""")
    )
  }

  @Test
  def `caseless starts_with works`: Unit = {
    test(
      analyzeStatement("SELECT text where caseless_starts_with(text, 'o')"),
      ("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ upper(text 'o') = left(upper(x1.text), length(upper(text 'o'))))""")
    )
  }

  @Test
  def `round works`: Unit = {
    test(
      analyzeStatement("SELECT text, round(num, 2)"),
      ("""SELECT x1.text AS i1, (/* soql_round */ round(x1.num, 2 :: decimal(30, 7) :: int) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `ToFloatingTimestamp`: Unit = {
    test(
      analyzeStatement(
        """select to_floating_timestamp("2022-12-31T23:59:59Z", "America/New_York")"""
      ),
      """SELECT (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York') AS i1 FROM table1 AS x1"""
    )
  }

  @Test
  def `FloatingTimeStampTruncYmd`: Unit = {
    test(
      analyzeStatement("select date_trunc_ymd('2022-12-31T23:59:59')"),
      ("""SELECT date_trunc('day', timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampTruncYm`: Unit = {
    test(
      analyzeStatement("select date_trunc_ym('2022-12-31T23:59:59')"),
      ("""SELECT date_trunc('month', timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampTruncY`: Unit = {
    test(
      analyzeStatement("select date_trunc_y('2022-12-31T23:59:59')"),
      ("""SELECT date_trunc('year', timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampZTruncYmd`: Unit = {
    test(
      analyzeStatement("select datez_trunc_ymd('2022-12-31T23:59:59Z')"),
      ("""SELECT date_trunc('day', timestamp with time zone '2022-12-31T23:59:59.000Z') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampZTruncYm`: Unit = {
    test(
      analyzeStatement("select datez_trunc_ym('2022-12-31T23:59:59Z')"),
      ("""SELECT date_trunc('month', timestamp with time zone '2022-12-31T23:59:59.000Z') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampZTruncY`: Unit = {
    test(
      analyzeStatement("select datez_trunc_y('2022-12-31T23:59:59Z')"),
      ("""SELECT date_trunc('year', timestamp with time zone '2022-12-31T23:59:59.000Z') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampTruncYmdAtTimeZone`: Unit = {
    test(
      analyzeStatement(
        "select date_trunc_ymd('2022-12-31T23:59:59Z', 'America/New_York')"
      ),
      ("""SELECT date_trunc('day', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York')) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampTruncYmAtTimeZone`: Unit = {
    test(
      analyzeStatement(
        "select date_trunc_ym('2022-12-31T23:59:59Z', 'America/New_York')"
      ),
      ("""SELECT date_trunc('month', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York')) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FixedTimeStampTruncYAtTimeZone`: Unit = {
    test(
      analyzeStatement(
        "select date_trunc_y('2022-12-31T23:59:59Z', 'America/New_York')"
      ),
      ("""SELECT date_trunc('year', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York')) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractY`: Unit = {
    test(
      analyzeStatement("select date_extract_y('2022-12-31T23:59:59')"),
      ("""SELECT extract(year from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractM`: Unit = {
    test(
      analyzeStatement("select date_extract_m('2022-12-31T23:59:59')"),
      ("""SELECT extract(month from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractD`: Unit = {
    test(
      analyzeStatement("select date_extract_d('2022-12-31T23:59:59')"),
      ("""SELECT extract(day from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractHh`: Unit = {
    test(
      analyzeStatement("select date_extract_hh('2022-12-31T23:59:59')"),
      ("""SELECT extract(hour from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractMm`: Unit = {
    test(
      analyzeStatement("select date_extract_mm('2022-12-31T23:59:59')"),
      ("""SELECT extract(minute from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractSs`: Unit = {
    test(
      analyzeStatement("select date_extract_ss('2022-12-31T23:59:59')"),
      ("""SELECT extract(second from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractDow`: Unit = {
    test(
      analyzeStatement("select date_extract_dow('2022-12-31T23:59:59')"),
      ("""SELECT extract(dayofweek from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimeStampExtractWoy`: Unit = {
    test(
      analyzeStatement("select date_extract_woy('2022-12-31T23:59:59')"),
      ("""SELECT extract(week from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `FloatingTimestampExtractIsoY`: Unit = {
    test(
      analyzeStatement("select date_extract_iso_y('2022-12-31T23:59:59')"),
      ("""SELECT extract(year from timestamp without time zone '2022-12-31T23:59:59.000') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `EpochSeconds`: Unit = {
    test(
      analyzeStatement("select epoch_seconds('2022-12-31T23:59:59Z')"),
      ("""SELECT extract(epoch from timestamp with time zone '2022-12-31T23:59:59.000Z') AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `TimeStampDiffD`: Unit = {
    test(
      analyzeStatement(
        "select date_diff_d('2022-12-31T23:59:59Z', '2022-01-01T00:00:00Z')"
      ),
      ("""SELECT datediff(day, timestamp with time zone '2022-12-31T23:59:59.000Z' at time zone (text 'UTC'), timestamp with time zone '2022-01-01T00:00:00.000Z' at time zone (text 'UTC')) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `SignedMagnitude10`: Unit = {
    test(
      analyzeStatement("select signed_magnitude_10(num)"),
      ("""SELECT ((/* soql_signed_magnitude_10 */ (sign(x1.num) * length(floor(abs(x1.num)) :: text)))) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `SignedMagnitudeLinear`: Unit = {
    test(
      analyzeStatement("select signed_magnitude_Linear(num, 8)"),
      ("""SELECT ((/* soql_signed_magnitude_linear */ (case when (8 :: decimal(30, 7)) = 1 then floor(x1.num) else sign(x1.num) * floor(abs(x1.num)/(8 :: decimal(30, 7)) + 1) end))) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `GetUtcDate`: Unit = {
    test(
      analyzeStatement("select get_utc_date()"),
      ("""SELECT current_date at time zone 'UTC' AS i1 FROM table1 AS x1""")
    )
  }

//  tests for aggregate functions
  @Test
  def `max works`: Unit = {
    test(
      analyzeStatement("SELECT max(num)"),
      ("""SELECT max(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `min works`: Unit = {
    test(
      analyzeStatement("SELECT min(num)"),
      ("""SELECT min(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `count(*) works`: Unit = {
    test(
      analyzeStatement("SELECT count(*)"),
      ("""SELECT (count(*)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `count() works`: Unit = {
    test(
      analyzeStatement("SELECT count(text)"),
      ("""SELECT (count(x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `count(iif ... ) works`: Unit = {
    test(
      analyzeStatement("SELECT count(IIF(text='one', 1, NULL))"),
      ("""SELECT (count(CASE WHEN (x1.text) = (text 'one') THEN 1 :: decimal(30, 7) ELSE null :: decimal(30, 7) END)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `count_distinct works`: Unit = {
    test(
      analyzeStatement("SELECT count_distinct(text)"),
      ("""SELECT (count(DISTINCT x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `sum works`: Unit = {
    test(
      analyzeStatement("SELECT sum(num)"),
      ("""SELECT sum(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `avg works`: Unit = {
    test(
      analyzeStatement("SELECT avg(num)"),
      ("""SELECT avg(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `median works`: Unit = {
    test(
      analyzeStatement("SELECT median(num)"),
      ("""SELECT median(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `stddev_pop works`: Unit = {
    test(
      analyzeStatement("SELECT stddev_pop(num)"),
      ("""SELECT stddev_pop(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `stddev_samp`: Unit = {
    test(
      analyzeStatement("SELECT stddev_samp(num)"),
      ("""SELECT stddev_samp(x1.num) AS i1 FROM table1 AS x1""")
    )
  }

//  tests for conditional functions
  @Test
  def `nullif works`: Unit = {
    test(
      analyzeStatement("SELECT text, nullif(num, 1)"),
      ("""SELECT x1.text AS i1, nullif(x1.num, 1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `coalesce works`: Unit = {
    test(
      analyzeStatement("SELECT coalesce(text, 'zero'), coalesce(num, '0')"),
      ("""SELECT coalesce(x1.text, text 'zero') AS i1, coalesce(x1.num, 0 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `case works`: Unit = {
    test(
      analyzeStatement(
        "SELECT num, case(num > 1, 'large num', num <= 1, 'small num')"
      ),
      ("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' WHEN (x1.num) <= (1 :: decimal(30, 7)) THEN text 'small num' END AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `iif works`: Unit = {
    test(
      analyzeStatement("SELECT num, iif(num > 1, 'large num', 'small num')"),
      ("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' ELSE text 'small num' END AS i2 FROM table1 AS x1""")
    )
  }

//  tests for geo functions
  @Test
  def `spacial union works for polygons and points`: Unit = {
    test(
      analyzeStatement("Select spatial_union(geom, geom)"),
      ("""SELECT st_asbinary(st_multi(st_union(x1.geom, x1.geom))) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `from line to multiline, from polygon to multipolygon and from line to multiline work`
      : Unit = {
    test(
      analyzeStatement("SELECT geo_multi(geom)"),
      ("""SELECT st_asbinary(st_multi(x1.geom)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `num_points works`: Unit = {
    test(
      analyzeStatement("SELECT num_points(geom)"),
      ("""SELECT (st_npoints(x1.geom)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `crosses works`: Unit = {
    test(
      analyzeStatement("SELECT crosses(geom, geom)"),
      ("""SELECT st_crosses(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `intersection works`: Unit = {
    test(
      analyzeStatement("SELECT polygon_intersection(geom, geom)"),
      ("""SELECT st_asbinary(st_multi(st_buffer(st_intersection(x1.geom, x1.geom), 0.0))) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `intersects works`: Unit = {
    test(
      analyzeStatement("SELECT intersects(geom, geom)"),
      ("""SELECT st_intersects(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `within_polygon works`: Unit = {
    test(
      analyzeStatement("SELECT within_polygon(geom, geom)"),
      ("""SELECT st_within(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `within_box works`: Unit = {
    test(
      analyzeStatement("SELECT text, within_box(geom, 23, 34, 10, 56)"),
      ("""SELECT x1.text AS i1, (/* within_box */ st_contains(st_makeenvelope(34 :: decimal(30, 7) :: DOUBLE PRECISION, 10 :: decimal(30, 7) :: DOUBLE PRECISION, 56 :: decimal(30, 7) :: DOUBLE PRECISION, 23 :: decimal(30, 7) :: DOUBLE PRECISION), x1.geom)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `is_empty works`: Unit = {
    test(
      analyzeStatement("SELECT text, is_empty(geom)"),
      ("""SELECT x1.text AS i1, st_isempty(x1.geom) or (x1.geom) is null AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `simplify works`: Unit = {
    test(
      analyzeStatement("SELECT text, simplify(geom, 1)"),
      ("""SELECT x1.text AS i1, st_asbinary(st_simplify(x1.geom, 1 :: decimal(30, 7))) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `area works`: Unit = {
    test(
      analyzeStatement("SELECT text, area(geom)"),
      ("""SELECT x1.text AS i1, (/* soql_area */ st_area(x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `distance_in_meters works for points only`: Unit = {
    test(
      analyzeStatement("SELECT text, distance_in_meters(geom, geom)"),
      ("""SELECT x1.text AS i1, (/* soql_distance_in_meters */ st_distance(x1.geom :: geography, x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `visible_at works`: Unit = {
    test(
      analyzeStatement("SELECT text, visible_at(geom, 3)"),
      ("""SELECT x1.text AS i1, (/* soql_visible_at */ (not st_isempty(x1.geom)) AND (st_geometrytype(x1.geom) = 'ST_Point' OR st_geometrytype(x1.geom) = 'ST_MultiPoint' OR (ST_XMax(x1.geom) - ST_XMin(x1.geom)) >= 3 :: decimal(30, 7) OR (ST_YMax(x1.geom) - ST_YMin(x1.geom)) >= 3 :: decimal(30, 7))) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `convex_hull works`: Unit = {
    test(
      analyzeStatement("SELECT text, convex_hull(geom)"),
      ("""SELECT x1.text AS i1, st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0))) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `curated_region_test works`: Unit = {
    test(
      analyzeStatement("Select text, curated_region_test(geom, 5)"),
      ("""SELECT x1.text AS i1, (/* soql_curated_region_test */ case when st_npoints(x1.geom) > 5 :: decimal(30, 7) then 'too complex' when st_xmin(x1.geom) < -180 or st_xmax(x1.geom) > 180 or st_ymin(x1.geom) < -90 or st_ymax(x1.geom) > 90 then 'out of bounds' when not st_isvalid(x1.geom) then 'invalid geography data' when x1.geom is null then 'empty' end) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `test for PointToLatitude`: Unit = {
    test(
      analyzeStatement("SELECT text, point_latitude(geometry_point)"),
      ("""SELECT x1.text AS i1, (st_y(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `test for PointToLongitude`: Unit = {
    test(
      analyzeStatement("SELECT text, point_longitude(geometry_point)"),
      ("""SELECT x1.text AS i1, (st_x(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1""")
    )
  }

//  tests for window functions
  @Test
  def `row_number works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text,num, row_number() over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, row_number() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `rank works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, rank() over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, rank() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i2 FROM table1 AS x1""")
    )
  }

  @Test
  def `dense_rank works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, dense_rank() over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, dense_rank() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `first_value works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, first_value(num) over(partition by text order by num rows between unbounded preceding and current row)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, first_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `last_value works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, last_value(num) over(partition by text order by num rows between unbounded preceding and current row)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, last_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `lead works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, lead(num) over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, lead(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `leadOffset works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, lead(num, 2) over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, lead(x1.num, (2 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `lag works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, lag(num) over(partition by text order by num desc)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, lag(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num DESC NULLS FIRST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `lagOffset works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, lag(num, 2) over(partition by text order by num desc)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, lag(x1.num, (2 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num DESC NULLS FIRST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `ntile works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, ntile(4) over(partition by text order by num)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, ntile((4 :: decimal(30, 7)) :: int) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) max works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, max(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, max(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) min works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, min(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, min(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) count(*) works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, count(*) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, (count(*) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) :: decimal(30, 7) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) count() works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, count(text) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, (count(x1.text) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) :: decimal(30, 7) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) sum works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, sum(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, sum(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) avg works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, avg(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, avg(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) median works`: Unit = {
    test(
      analyzeStatement("SELECT text, num, median(num) over(partition by text)"),
      ("""SELECT x1.text AS i1, x1.num AS i2, median(x1.num) OVER (PARTITION BY x1.text) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) stddev_pop works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, stddev_pop(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, stddev_pop(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  @Test
  def `(window function) stddev_samp works`: Unit = {
    test(
      analyzeStatement(
        "SELECT text, num, stddev_samp(num) over(partition by text order by num rows between unbounded preceding and unbounded following)"
      ),
      ("""SELECT x1.text AS i1, x1.num AS i2, stddev_samp(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i3 FROM table1 AS x1""")
    )
  }

  //  tests for geo-casts
  @Test
  def `geo cast text to point works`: Unit = {
    assertEquals(
      analyzeStatement("SELECT ('POINT' || '(0 9)') :: point"),
      ("""SELECT st_asbinary(st_geomfromtext((text 'POINT') || (text '(0 9)'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `geo cast text to multipoint works`: Unit = {
    assertEquals(
      analyzeStatement(
        "SELECT ('MULTIPOINT' || '((0 0), (1 1))') :: multipoint"
      ),
      ("""SELECT st_asbinary(st_geomfromtext((text 'MULTIPOINT') || (text '((0 0), (1 1))'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `geo cast text to line works`: Unit = {
    assertEquals(
      analyzeStatement("SELECT ('LINESTRING' || '(0 0, 0 1, 1 2)') :: line"),
      ("""SELECT st_asbinary(st_geomfromtext((text 'LINESTRING') || (text '(0 0, 0 1, 1 2)'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `geo cast text to multiline works`: Unit = {
    assertEquals(
      analyzeStatement(
        "SELECT ('MULTILINESTRING' || '((0 0, 1 1), (2 2, 3 3))') :: multiline"
      ),
      ("""SELECT st_asbinary(st_geomfromtext((text 'MULTILINESTRING') || (text '((0 0, 1 1), (2 2, 3 3))'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `geo cast text to polygon works`: Unit = {
    assertEquals(
      analyzeStatement(
        "SELECT ('POLYGON' || '((0 0, 1 0, 1 1, 0 1, 0 0))') :: polygon"
      ),
      ("""SELECT st_asbinary(st_geomfromtext((text 'POLYGON') || (text '((0 0, 1 0, 1 1, 0 1, 0 0))'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  @Test
  def `geo cast text to multipolygon works`: Unit = {
    assertEquals(
      analyzeStatement(
        "SELECT ('MULTIPOLYGON' || '(((1 1, 1 3, 3 3, 3 1, 1 1)), ((4 3, 6 3, 6 1, 4 1, 4 3)))') :: multipolygon"
      ),
      ("""SELECT st_asbinary(st_geomfromtext((text 'MULTIPOLYGON') || (text '(((1 1, 1 3, 3 3, 3 1, 1 1)), ((4 3, 6 3, 6 1, 4 1, 4 3)))'), 4326)) AS i1 FROM table1 AS x1""")
    )
  }

  //  tests for simple casts

  @Test
  def `text to boolean cast works`(): Unit = {
    assertEquals(
      analyzeStatement("SELECT ('TR' || 'UE') :: boolean"),
      """SELECT (/* TextToBool */ (case when lower((text 'TR') || (text 'UE')) = 'true' then true else false end)) AS i1 FROM table1 AS x1"""
    )
  }

  @Test
  def `text to numeric cast works`(): Unit = {
    assertEquals(
      analyzeStatement("SELECT ('5' || '4') :: number"),
      """SELECT ((text '5') || (text '4')) :: decimal(30, 7) AS i1 FROM table1 AS x1"""
    )
  }

  @Test
  def `number to text cast works`(): Unit = {
    assertEquals(
      analyzeStatement("SELECT (5 + 4) :: text"),
      """SELECT ((5 :: decimal(30, 7)) + (4 :: decimal(30, 7))) :: text AS i1 FROM table1 AS x1"""
    )
  }

  @Test
  def `text to fixed timestamp works`(): Unit = {
    assertEquals(
      analyzeStatement(
        "Select ('2022-12-31T' || '23:59:59Z') :: fixed_timestamp"
      ),
      """SELECT ((text '2022-12-31T') || (text '23:59:59Z')) :: timestamp with time zone AS i1 FROM table1 AS x1"""
    )
  }

  @Test
  def `text to floating timestamp works`(): Unit = {
    assertEquals(
      analyzeStatement(
        "SELECT ('2022-12-31T' || '23:59:59Z') :: floating_timestamp"
      ),
      """SELECT ((text '2022-12-31T') || (text '23:59:59Z')) :: timestamp without time zone AS i1 FROM table1 AS x1"""
    )
  }
}
