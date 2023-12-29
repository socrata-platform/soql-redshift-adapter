package com.socrata.service

import com.socrata.db.Exists
import com.socrata.store.blob.Inserter
import com.socrata.db.meta.entity._
import com.rojoma.simplearm.v2.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata.IndexDirective
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.service._
import jakarta.transaction.Transactional

@ApplicationScoped
class RedshiftSecondary(
    datasetService: DatasetService,
    datasetColumnService: DatasetColumnService,
    inserter: Inserter
) extends Secondary[SoQLType, SoQLValue] {
  override def shutdown(): Unit = ???

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = ???

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = ???

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = ???

  override def version(info: VersionInfo[SoQLType, SoQLValue]): Cookie = {
    None
  }

  /*


   Transactional does not work. This still inserts



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
      isLatestLivingCopy: Boolean): Cookie = {

    val (dataset, columns) =
      datasetService.persist(Dataset(datasetInfo, copyInfo)) match {
        case Exists.Does(dataset) => (???, ???)
        // delete it and recreate it.
        case Exists.DoesNot(dataset) => {
          val columns: List[DatasetColumn] = schema.values.map(columnInfo =>
            datasetColumnService.persist(DatasetColumn(datasetInfo, copyInfo, columnInfo)) match {
              case Exists.Does(column) =>
                throw new IllegalStateException(s"column $column existed on a dataset that did not exist.")
                ???
              case Exists.DoesNot(column) => column
            }
          ).toList
          (dataset, columns)
        }
      }

    rows.foreach { rows: Iterator[ColumnIdMap[SoQLValue]] =>
      inserter.store(dataset, schema, rows)
    }
    throw new Throwable(":(")
    None
  }

  override def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie =
    ???
}

/*

 types are a mess
 package names are a mess.
 not sure where to use db entities and where to not.
 inserter poorly named
 transactions not working
 id is silly

 json does not support GEO
 CSV does support GEO, but writing CSV is difficult. Cannot get CsvWRiter to write to s3.

 imports are out of order and importing too much (scalafix would be nice)

 need to work out config. Not sure how to inject thing which requires partSize, for example.

 */
