package service

import com.rojoma.simplearm.v2.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata.IndexDirective
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import _root_.config.RedshiftSecondaryConfig
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import jakarta.enterprise.context.ApplicationScoped
import meta.service.MetaService


@ApplicationScoped
class RedshiftSecondary(
                         metaService:MetaService,
                         config: RedshiftSecondaryConfig
                       ) extends Secondary[SoQLType, SoQLValue] {
  override def shutdown(): Unit = ???

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = ???

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = ???

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = ???

  override def version(info: VersionInfo[SoQLType, SoQLValue]): Cookie = ???

  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Cookie, rows: Managed[Iterator[ColumnIdMap[SoQLValue]]], rollups: Seq[RollupInfo], indexDirectives: Seq[IndexDirective[SoQLType]], indexes: Seq[IndexInfo], isLatestLivingCopy: Boolean): Cookie = ???

  override def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie = ???
}
