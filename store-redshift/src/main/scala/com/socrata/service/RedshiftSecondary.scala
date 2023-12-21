package com.socrata.service

import com.rojoma.simplearm.v2.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata.IndexDirective
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.db.meta.service._

@ApplicationScoped
class RedshiftSecondary(
  datasetService: DatasetService
) extends Secondary[SoQLType, SoQLValue] {
  override def shutdown(): Unit = ???

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = ???

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = ???

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = ???

  // ColumnCreatedHandler, where it makes a call to physicalColumnBaseBase, instead of passing in secColInfo.id.underlying pass in secColInfo.fieldName.underlying and it should all Just Work.
  // https://teams.microsoft.com/l/message/19:f6588672e1684823bec41fceac1e55ba@thread.tacv2/1699997650096?tenantId=7cc5f0f9-ee5b-4106-a62d-1b9f7be46118&groupId=102da3ef-c928-4a59-afe5-f0d51e6443dd&parentMessageId=1699995352754&teamName=D%26I%20-%20Internal&channelName=siq_internal&createdTime=1699997650096&allowXTenantAccess=false

  override def version(info: VersionInfo[SoQLType, SoQLValue]): Cookie = {
    None
  }

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

    datasetService.insert(???)

    None
  }

  override def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie =
    ???
}
