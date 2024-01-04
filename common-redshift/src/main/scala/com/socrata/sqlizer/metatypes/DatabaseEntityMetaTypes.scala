package com.socrata.common.sqlizer.metatypes

import scala.collection.{mutable => scm}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.id.{CopyId, DatasetId, UserColumnId}
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.functions.SoQLTypeInfo2

final class DatabaseEntityMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = Unit
  override type DatabaseColumnNameImpl = Unit

  val typeInfo = new SoQLTypeInfo2[DatabaseEntityMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[DatabaseEntityMetaTypes] {
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]
    private val provMap = new scm.HashMap[(DatasetId, CopyId), Provenance]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseEntityMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseEntityMetaTypes]): Provenance = {
      provMap.get((???, ???)) match {
        case Some(existing) =>
          existing
        case None =>
          val prov = Provenance(dtnMap.size.toString)
          provMap += (???, ???) -> prov
          dtnMap += prov -> dtn
          prov
      }
    }
  }

  def rewriteFrom[MT <: MetaTypes with ({
    type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = UserColumnId
  })](
      analysis: SoQLAnalysis[MT],
      fromProv: types.FromProvenance[MT]
  )(
      implicit
      changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, DatabaseEntityMetaTypes]): SoQLAnalysis[DatabaseEntityMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseEntityMetaTypes](
      { dtn => DatabaseTableName(???) }, // TODO proper error
      { case (dtn, DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(???) // TODO proper errors
      },
      fromProv,
      provenanceMapper,
      typeInfo.updateProvenance
    )
  }
}
