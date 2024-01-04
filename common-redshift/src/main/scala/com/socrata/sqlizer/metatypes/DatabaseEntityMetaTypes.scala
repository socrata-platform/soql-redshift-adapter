package com.socrata.common.sqlizer.metatypes

import scala.collection.{mutable => scm}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo}
import com.socrata.datacoordinator.id.{DatasetInternalName, DatasetId, UserColumnId}
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.functions.SoQLTypeInfo2
import com.socrata.db

final class DatabaseEntityMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = db.meta.entity.Dataset
  override type DatabaseColumnNameImpl = db.meta.entity.DatasetColumn

  val typeInfo = new SoQLTypeInfo2[DatabaseEntityMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[DatabaseEntityMetaTypes] {
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]
    private val provMap = new scm.HashMap[(DatasetId, Long /* Dataset.systemId */ ), Provenance]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseEntityMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseEntityMetaTypes]): Provenance = {
      // TODO
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

  def rewriteFrom(
      analysis: SoQLAnalysis[InputMetaTypes],
      fromProv: types.FromProvenance[InputMetaTypes]
  ): SoQLAnalysis[DatabaseEntityMetaTypes] = {

    analysis.rewriteDatabaseNames[DatabaseEntityMetaTypes](
      { case DatabaseTableName((DatasetInternalName(instance, datasetId), stage)) =>
        DatabaseTableName(db.meta.entity.Dataset(???, ???)) // TODO
      },
      { case (DatabaseTableName((DatasetInternalName(instance, datasetId), stage)), DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(db.meta.entity.DatasetColumn(???, ???, ???, ???)) // TODO
      },
      fromProv,
      provenanceMapper,
      typeInfo.updateProvenance
    )
  }

}
