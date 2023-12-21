package com.socrata.common.sqlizer.metatypes

import scala.collection.{mutable => scm}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.id.{CopyId, DatasetId, UserColumnId}
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.functions.SoQLTypeInfo2

// delete this file
final class DatabaseMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = CopyInfo
  override type DatabaseColumnNameImpl = ColumnInfo[ColumnType]

  val typeInfo = new SoQLTypeInfo2[DatabaseMetaTypes]

  // ugh.. but making this stateful was the only way I could find to
  // do this.
  val provenanceMapper = new types.ProvenanceMapper[DatabaseMetaTypes] {
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]
    private val provMap = new scm.HashMap[(DatasetId, CopyId), Provenance]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseMetaTypes]): Provenance = {
      val provKey = (dtn.name.datasetInfo.systemId, dtn.name.systemId)
      provMap.get(provKey) match {
        case Some(existing) =>
          existing
        case None =>
          val prov = Provenance(dtnMap.size.toString)
          provMap += provKey -> prov
          dtnMap += prov -> dtn
          prov
      }
    }
  }

  def rewriteFrom[MT <: MetaTypes with ({
    type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = UserColumnId
  })](
      analysis: SoQLAnalysis[MT],
      copyCache: CopyCache[MT],
      fromProv: types.FromProvenance[MT]
  )(
      implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, DatabaseMetaTypes]): SoQLAnalysis[DatabaseMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseMetaTypes](
      { dtn => DatabaseTableName(copyCache(dtn).get._1) }, // TODO proper error
      { case (dtn, DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(copyCache(dtn).get._2.get(userColumnId).get) // TODO proper errors
      },
      fromProv,
      provenanceMapper,
      typeInfo.updateProvenance
    )
  }
}
