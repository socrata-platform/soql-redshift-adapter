package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.sqlizer._
import com.socrata.common.sqlizer._
import com.socrata.soql.analyzer2._

import com.socrata.common.sqlizer.metatypes.DatabaseNamesMetaTypes

object RedshiftSqlizer
    extends Sqlizer[DatabaseNamesMetaTypes](
      new SoQLFunctionSqlizerRedshift[DatabaseNamesMetaTypes],
      new RedshiftExprSqlFactory[DatabaseNamesMetaTypes],
      RedshiftNamespaces,
      new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true),
      DatabaseNamesMetaTypes.provenanceMapper,
      _ => false, // remove this
      (sqlizer, _, extraContext) =>
        SoQLSqlizer.repProvider(
          extraContext.cryptProviderProvider,
          extraContext.escapeString,
          sqlizer.toProvenance,
          sqlizer.namespace,
          sqlizer.exprSqlFactory
        )
    )

object SoQLSqlizer {
  def repProvider(
      cryptProviderProvider: CryptProviderProvider, // default
      escapeString: String => String, // default
      toProvenance: types.ToProvenance[DatabaseNamesMetaTypes],
      namespace: SqlNamespaces[DatabaseNamesMetaTypes] = RedshiftNamespaces,
      exprSqlFactory: ExprSqlFactory[DatabaseNamesMetaTypes] = new RedshiftExprSqlFactory) =
    new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
      cryptProviderProvider,
      namespace,
      exprSqlFactory,
      toProvenance
    ) {
      override def mkStringLiteral(text: String): Doc = {
        // By default, converting a String to Doc replaces the newlines
        // with soft newlines which can be converted into spaces by
        // `group`.  This is a thing we _definitely_ don't want, so
        // instead replace those newlines with hard line breaks, and
        // un-nest lines by the current nesting level so the linebreak
        // doesn't introduce any indentation.
        val escapedText = escapeString(text)
          .split("\n", -1)
          .toSeq
          .map(Doc(_))
          .concatWith { (a: Doc, b: Doc) => a ++ Doc.hardline ++ b }
        val unindented = Doc.nesting { depth => escapedText.nest(-depth) }
        d"'" ++ unindented ++ d"'"
      }
    }
}
