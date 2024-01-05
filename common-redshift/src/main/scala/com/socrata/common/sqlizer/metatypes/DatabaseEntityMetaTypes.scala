package com.socrata.common.sqlizer.metatypes

import com.socrata.common.db.meta.entity.{Dataset, DatasetColumn}
import com.socrata.common.db.meta.service.{DatasetColumnService, DatasetService}
import com.socrata.datacoordinator.id._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.functions.SoQLTypeInfo2
import com.socrata.soql.types.{SoQLType, SoQLValue}
import jakarta.enterprise.context.ApplicationScoped

import scala.collection.{mutable => scm}

@ApplicationScoped
final class DatabaseEntityMetaTypes(
    datasetService: DatasetService,
    columnService: DatasetColumnService)
    extends MetaTypes {

  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = Dataset
  override type DatabaseColumnNameImpl = DatasetColumn

  val typeInfo = new SoQLTypeInfo2[DatabaseEntityMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[DatabaseEntityMetaTypes] {
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]
    private val provMap = new scm.HashMap[(String, Long /* Dataset.internalName, CopyNumber */ ), Provenance]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseEntityMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseEntityMetaTypes]): Provenance = {
      val provKey = (dtn.name.internalName, dtn.name.copyNumber)
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

  def rewriteFrom(
      analysis: SoQLAnalysis[InputMetaTypes],
      fromProv: types.FromProvenance[InputMetaTypes]
  ): SoQLAnalysis[DatabaseEntityMetaTypes] = {

    analysis.rewriteDatabaseNames[DatabaseEntityMetaTypes](
      { case DatabaseTableName((dsid, stage)) =>
        DatabaseTableName(datasetService.findByInternalNameAndPublishedState(
          dsid.underlying,
          stage.underlying
        ).get)
      },
      { case (DatabaseTableName((dsid, stage)), DatabaseColumnName(colId: UserColumnId)) =>
        DatabaseColumnName {
          val dataset = datasetService.findByInternalNameAndPublishedState(
            dsid.underlying,
            stage.underlying
          ).get

          columnService.findByDatasetIdAndUserColumnId(
            dataset.systemId,
            colId.underlying
          ).get
        }
      },
      fromProv,
      provenanceMapper,
      typeInfo.updateProvenance
    )
  }

}
