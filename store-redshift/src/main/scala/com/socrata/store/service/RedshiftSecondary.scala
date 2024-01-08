package com.socrata.store.service

import com.socrata.store.handlers._
import com.rojoma.simplearm.v2.Managed
import com.socrata.common.db.Exists
import com.socrata.common.db.meta.entity.{Dataset, DatasetColumn}
import com.socrata.common.db.meta.service.{DatasetColumnService, DatasetService}
import com.socrata.db.datasets
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata.IndexDirective
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.transaction.Transactional

@ApplicationScoped
class RedshiftSecondary(
    tableDeleter: datasets.TableDeleter,
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

    val (dataset, columns) =
      datasetService.persist(Dataset(datasetInfo, copyInfo)) match {
        case Exists.Updated(dataset) =>
          tableDeleter.delete(dataset)
          return resync(
            datasetInfo,
            copyInfo,
            schema,
            cookie,
            rows,
            rollups,
            indexDirectives,
            indexes,
            isLatestLivingCopy
          )

        case Exists.Inserted(dataset) => {
          val columns: List[DatasetColumn] = schema.values
            .map(columnInfo =>
              datasetColumnService.persist(
                DatasetColumn(dataset, columnInfo)
              ) match {
                case Exists.Updated(column) =>
                  throw new IllegalStateException(
                    s"column $column existed on a dataset that did not exist."
                  )
                case Exists.Inserted(column) => column
              }
            )
            .toList
          (dataset, columns)
        }
      }

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

/*

 not sure where to use db entities and where to not.
 inserter poorly named
 transactions not working
 id is silly

 json does not support GEO
 CSV does support GEO, but writing CSV is difficult. Cannot get CsvWRiter to write to s3.

 imports are out of order and importing too much (scalafix would be nice)

 need to work out config. Not sure how to inject thing which requires partSize, for example.

 */
