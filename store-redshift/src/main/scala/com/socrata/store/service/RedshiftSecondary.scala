package com.socrata.store.service

import com.socrata.store.handlers._
import com.rojoma.simplearm.v2.Managed
import com.socrata.common.db.meta.entity.{Dataset, DatasetColumn}
import com.socrata.common.db.meta.service.{DatasetColumnService, DatasetService}
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata.IndexDirective
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@ApplicationScoped
class RedshiftSecondary(
    datasetService: DatasetService,
    datasetColumnService: DatasetColumnService,
    resyncHandler: Resync
) extends Secondary[SoQLType, SoQLValue] {
  override def shutdown(): Unit = ???

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit =
    ???

  override def currentVersion(
      datasetInternalName: String,
      cookie: Cookie
  ): Long = ???

  override def currentCopyNumber(
      datasetInternalName: String,
      cookie: Cookie
  ): Long = ???

  override def version(info: VersionInfo[SoQLType, SoQLValue]): Cookie = {
    None
  }

  /*

   transactional continues to not work

   */
  @Transactional
  override def resync(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      cookie: Cookie,
      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
      rollups: Seq[RollupInfo],
      indexDirectives: Seq[IndexDirective[SoQLType]],
      indexes: Seq[IndexInfo],
      isLatestLivingCopy: Boolean
  ): Cookie = {

    val dataset = datasetService.persist(Dataset(datasetInfo, copyInfo))
    val columns: List[DatasetColumn] = schema.values
      .map(columnInfo =>
        datasetColumnService.persist(
          DatasetColumn(dataset, columnInfo)
        )
      )
      .toList

    rows.foreach { rows: Iterator[ColumnIdMap[SoQLValue]] =>
      resyncHandler.store(dataset, columns, schema, rows)
    }
    None
  }

  override def dropCopy(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      cookie: Cookie,
      isLatestCopy: Boolean
  ): Cookie =
    ???
}
