package com.socrata.common.sqlizer.metatypes

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.SoQLValue
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.SoQLTypeInfo2

final abstract class DatabaseNamesMetaTypes extends MetaTypes with SoQLMetaTypesExt {
  override type ResourceNameScope = DatabaseMetaTypes#ResourceNameScope
  override type ColumnType = DatabaseMetaTypes#ColumnType
  override type ColumnValue = DatabaseMetaTypes#ColumnValue
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

  def rewriteDTN(dtn: types.DatabaseTableName[DatabaseMetaTypes]): types.DatabaseTableName[DatabaseNamesMetaTypes] =
    dtn match {
      case DatabaseTableName(copyInfo) => DatabaseTableName(copyInfo.dataTableName)
    }

  def rewriteFrom(dmtState: DatabaseMetaTypes, analysis: SoQLAnalysis[DatabaseMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] =
    analysis.rewriteDatabaseNames[DatabaseNamesMetaTypes](
      rewriteDTN,
      { case (dtn, DatabaseColumnName(columnInfo)) => DatabaseColumnName(columnInfo.physicalColumnBase) },
      dmtState.provenanceMapper,
      provenanceMapper,
      typeInfo.updateProvenance
    )
}
