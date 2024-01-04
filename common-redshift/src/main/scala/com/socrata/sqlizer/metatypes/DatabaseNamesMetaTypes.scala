package com.socrata.common.sqlizer.metatypes

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.functions.SoQLTypeInfo2
import com.socrata.soql.types.{SoQLType, SoQLValue}

final abstract class DatabaseNamesMetaTypes extends MetaTypes with SoQLMetaTypesExt {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = String
  override type DatabaseColumnNameImpl = String
}

object DatabaseNamesMetaTypes extends MetaTypeHelper[DatabaseNamesMetaTypes] {
  val typeInfo = new SoQLTypeInfo2[DatabaseNamesMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[DatabaseNamesMetaTypes] {
    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseNamesMetaTypes] =
      DatabaseTableName(prov.value)

    def toProvenance(dtn: types.DatabaseTableName[DatabaseNamesMetaTypes]): Provenance =
      dtn match {
        case DatabaseTableName(name) => Provenance(name)
      }
  }

  def rewriteDTN(dtn: types.DatabaseTableName[InputMetaTypes]): types.DatabaseTableName[DatabaseNamesMetaTypes] =
    ???

  def rewriteFrom(
      dmtState: InputMetaTypes,
      analysis: SoQLAnalysis[InputMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] =
    ???
}
