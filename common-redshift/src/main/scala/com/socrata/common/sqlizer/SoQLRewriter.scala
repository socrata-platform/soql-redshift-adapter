package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.sqlizer._
import com.socrata.common.sqlizer._
import com.socrata.soql.analyzer2._

import com.socrata.common.sqlizer.metatypes._

@jakarta.enterprise.context.ApplicationScoped
class SoQLRewriter(databaseEntityMetaTypes: DatabaseEntityMetaTypes) {
  def rewrite(analysis: SoQLAnalysis[InputMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] = {
    val analysis2 = databaseEntityMetaTypes.rewriteFrom(analysis, InputMetaTypes.provenanceMapper)
    DatabaseNamesMetaTypes.rewriteFrom(databaseEntityMetaTypes, analysis2)
  }
}