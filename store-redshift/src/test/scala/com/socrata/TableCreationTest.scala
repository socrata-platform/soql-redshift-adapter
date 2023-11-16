package com.socrata.store.sqlizer

import com.socrata.soql.parsing._
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
import com.socrata.common.sqlizer._


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
}

class TableCreationTest  {

  type TestMT = TableCreationTest.TestMT

  val sqlizer = TableCreationTest.TestSqlizer
  val funcallSqlizer = TableCreationTest.TestFuncallSqlizer
  val repProvider = TableCreationTest.TestRepProvider


  @Test
  def foo(): Unit = {
    implicit val hasType: HasType[TableCreationTest.TestMT#ColumnValue, TableCreationTest.TestMT#ColumnType]  = new HasType[TableCreationTest.TestMT#ColumnValue, TableCreationTest.TestMT#ColumnType] {
      def typeOf(cv: TableCreationTest.TestMT#ColumnValue): TableCreationTest.TestMT#ColumnType = cv.typ
    }

    println(repProvider.reps(SoQLUrl).compressedSubColumns("table1", DatabaseColumnName("text")))
//    println(repProvider.reps(SoQLNumber).literal(LiteralValue[TestMT](SoQLNumber(new java.math.BigDecimal(22)))(new AtomicPositionInfo(SoQLPosition(0, 0, "", 0), SoQLPosition(0, 0, "", 0)))))
  }
}
