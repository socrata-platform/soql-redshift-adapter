package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.sqlizer._

import com.socrata.common.sqlizer.metatypes.DatabaseNamesMetaTypes

object RedshiftSqlizer extends Sqlizer[DatabaseNamesMetaTypes](
  new SoQLFunctionSqlizerRedshift[DatabaseNamesMetaTypes],
  new RedshiftExprSqlFactory[DatabaseNamesMetaTypes],
  RedshiftNamespaces,
  new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true),
  DatabaseNamesMetaTypes.provenanceMapper,
  _ => false, // remove this
  (sqlizer, _, extraContext) => new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
    extraContext.cryptProviderProvider,
    sqlizer.namespace,
    sqlizer.exprSqlFactory
  ) {
    override def mkStringLiteral(text: String): Doc = {
      // By default, converting a String to Doc replaces the newlines
      // with soft newlines which can be converted into spaces by
      // `group`.  This is a thing we _definitely_ don't want, so
      // instead replace those newlines with hard line breaks, and
      // un-nest lines by the current nesting level so the linebreak
      // doesn't introduce any indentation.
      val escapedText = extraContext.escapeString(text)
        .split("\n", -1)
        .toSeq
        .map(Doc(_))
        .concatWith { (a: Doc, b: Doc) =>
        a ++ Doc.hardline ++ b
      }
      val unindented = Doc.nesting { depth => escapedText.nest(-depth) }
      d"'" ++ unindented ++ d"'"
    }
  }
)
