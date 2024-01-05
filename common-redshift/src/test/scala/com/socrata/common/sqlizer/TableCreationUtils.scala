package com.socrata.common.sqlizer

import com.socrata.common.sqlizer.metatypes._

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._

import com.socrata.soql.environment.Provenance

import com.socrata.common.sqlizer._

trait TableCreationUtils {
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
    protected override def idxPrefix: String = "idx"
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
    (sqlizer, _, extraContext) =>
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

  implicit val hasType: HasType[DatabaseNamesMetaTypes#ColumnValue, DatabaseNamesMetaTypes#ColumnType] =
    new HasType[DatabaseNamesMetaTypes#ColumnValue, DatabaseNamesMetaTypes#ColumnType] {
      def typeOf(cv: DatabaseNamesMetaTypes#ColumnValue): DatabaseNamesMetaTypes#ColumnType = cv.typ
    }
}

/*
do compound types
do ID and version

test various soql commands that may fail due to super stuff
 */
