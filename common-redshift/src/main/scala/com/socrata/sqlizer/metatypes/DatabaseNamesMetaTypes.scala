package com.socrata.common.sqlizer.metatypes

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.functions.SoQLTypeInfo2

final abstract class DatabaseNamesMetaTypes extends MetaTypes with SoQLMetaTypesExt {
  override type ResourceNameScope = DatabaseEntityMetaTypes#ResourceNameScope
  override type ColumnType = DatabaseEntityMetaTypes#ColumnType
  override type ColumnValue = DatabaseEntityMetaTypes#ColumnValue
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

  def rewriteDTN(dtn: types.DatabaseTableName[DatabaseEntityMetaTypes]): types.DatabaseTableName[DatabaseNamesMetaTypes] =
    dtn match {
      case DatabaseTableName(copyInfo) => DatabaseTableName(??? /* dataset.table */ )
    }

  def rewriteFrom(
      dmtState: DatabaseEntityMetaTypes,
      analysis: SoQLAnalysis[DatabaseEntityMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] =
    analysis.rewriteDatabaseNames[DatabaseNamesMetaTypes](
      rewriteDTN,
      { case (_, DatabaseColumnName(columnInfo)) => DatabaseColumnName(??? /* datasetColumn.columnName */ ) },
      dmtState.provenanceMapper,
      provenanceMapper,
      typeInfo.updateProvenance
    )
}
