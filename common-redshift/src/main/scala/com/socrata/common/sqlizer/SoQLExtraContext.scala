package com.socrata.common.sqlizer

import org.joda.time.DateTime

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer.ExtraContext

import com.socrata.common.sqlizer.metatypes.DatabaseNamesMetaTypes

class SoQLExtraContext(
    val systemContext: Map[String, String],
    val cryptProviderProvider: CryptProviderProvider, // remove?
    val escapeString: String => String // remove?
) extends ExtraContext[SoQLExtraContext.Result] {
  val now = DateTime.now()
  var nonliteralSystemContextLookupFound: Boolean = false
  var nowUsed = false

  override def finish() =
    SoQLExtraContext.Result(
      nonliteralSystemContextLookupFound,
      if (nowUsed) Some(now) else None
    )
}

object SoQLExtraContext extends StatementUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]

  case class Result(nonliteralSystemContextLookupFound: Boolean, now: Option[DateTime])
}
