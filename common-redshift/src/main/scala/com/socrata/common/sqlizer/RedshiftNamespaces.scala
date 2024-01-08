package com.socrata.common.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._

import com.socrata.common.sqlizer.metatypes.DatabaseNamesMetaTypes

object RedshiftNamespaces extends SqlNamespaces[DatabaseNamesMetaTypes] {
  override def rawDatabaseTableName(dtn: DatabaseTableName) = {
    val DatabaseTableName(dataTableName) = dtn
    dataTableName
  }

  override def rawDatabaseColumnBase(dcn: DatabaseColumnName) = {
    val DatabaseColumnName(physicalColumnBase) = dcn
    physicalColumnBase
  }

  override def gensymPrefix: String = "g"

  protected override def idxPrefix: String = "idx" // will we even have indices?

  protected override def autoTablePrefix: String =
    "x" // "t" is taken by physical tables

  protected override def autoColumnPrefix: String = "i"
}
